// A fully integrated event-sourced API server using inline LSS and Functional Core (greenfield)

import http from "http"
import crypto from "crypto"
import pg from "pg"


// ---------- LSS Implementation ----------
const lss = await (async () => {
  return {
    singleWriter: await (async () => {
      const writeDb = new pg.Client({ connectionString: process.env.DATABASE_URL || 'postgres://postgres:12345@localhost:5432/postgres' })
      await writeDb.connect()
      const tables = await writeDb.query(`SELECT * FROM information_schema.tables WHERE table_name = 'lss'`)
      if (tables.rows.length === 0) {
        await writeDb.query(`
            CREATE TABLE lss (
                                 lss_order_id bigint NOT NULL,
                                 lss_partition_id VARCHAR(255) NOT NULL,
                                 lss_type VARCHAR(255) NOT NULL,
                                 lss_data JSONB NOT NULL,
                                 lss_metadata JSONB NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS lss_order_idx ON lss(lss_order_id);
            CREATE UNIQUE INDEX IF NOT EXISTS lss_partition_order_idx ON lss(lss_partition_id, lss_order_id);
            INSERT INTO lss (lss_order_id, lss_partition_id, lss_type, lss_data, lss_metadata)
            VALUES (0, 'system', 'LSS.Initialized', '{}', '{}');
        `)
      }
      const { rows: maxRows } = await writeDb.query(`SELECT COALESCE(MAX(lss_order_id), 0) AS max FROM lss`)
      let currentOrderId = maxRows[0].max
      return {
        physicalAppend: async (events) => {
          const appendTime = new Date().toISOString()
          const values = events.map(({ partitionId, type, data, metadata }) => {
            metadata.appendTime = appendTime
            return `(${++currentOrderId}, '${partitionId}', '${type}', $$${JSON.stringify(data).replace(/\$/g, '$')}$$, $$${JSON.stringify(metadata)}$$)`
          }).join(',')
          await writeDb.query(`
              INSERT INTO lss (lss_order_id, lss_partition_id, lss_type, lss_data, lss_metadata)
              VALUES ${values}
          `)
        }
      }
    })(),
    sharedReader: await (async () => {
      const readPool = new pg.Pool({ connectionString: process.env.DATABASE_URL || 'postgres://postgres:12345@localhost:5432/postgres' })
      return {
        physicalRead: async () => {
          const { rows } = await readPool.query(`SELECT * FROM lss ORDER BY lss_order_id ASC`)
          return rows.map(row => ({
            type: row.lss_type,
            data: row.lss_data,
            metadata: row.lss_metadata
          }))
        },
        logicalRead: async (partitionId, asc = true, limit = 100, offset = 0) => {
          const order = asc ? 'ASC' : 'DESC'
          const { rows } = await readPool.query(`
              SELECT lss_type AS type, lss_data AS data, lss_metadata AS metadata FROM lss WHERE lss_partition_id = $1
              ORDER BY lss_order_id ${order}
              LIMIT $2 OFFSET $3`, [partitionId, limit, offset])
          return rows
        },
        logicalReadFirst: async (partitionId) => {
          const rows = await lss.sharedReader.logicalRead(partitionId, true, 1, 0)
          if (rows.length === 0) throw new Error(`Partition ${partitionId} is empty`)
          return rows[0]
        },
        logicalReadLast: async (partitionId) => {
          const rows = await lss.sharedReader.logicalRead(partitionId, false, 1, 0)
          if (rows.length === 0) throw new Error(`Partition ${partitionId} is empty`)
          return rows[0]
        }
      }
    })()
  }
})()

// ---------- Functional Core Implementation ----------

const core = (() => {
  const StateChange = [
    {
      viewId: 'Subscription.Create',
      initialState: { nextId: 1 },
      reduce: {
        'Subscription.Created': (state, event) => {
          state.nextId = Math.max(state.nextId, parseInt(event.data.subscriptionId.split('-')[1]) + 1)
        }
      },
      map: (command, state) => [{
        type: 'Subscription.Created',
        data: {
          subscriptionId: `sub-${state.nextId++}`,
          plan: command.plan,
          createdBy: command.createdBy
        },
        metadata: {}
      }]
    },
    {
      viewId: 'Subscription.Assign.Members',
      initialState: {},
      reduce: {},
      map: (command, state) => [{
        type: 'Members.AssignmentStarted',
        data: {
          subscriptionId: command.subscriptionId,
          members: command.members
        },
        metadata: {}
      }]
    }
  ]

  const StateView = [
    {
      viewId: 'Subscription.List',
      initialState: {},
      reduce: {
        'Subscription.Created': (state, data) => {
          state[data.subscriptionId] = {
            plan: data.plan,
            createdBy: data.createdBy,
            members: []
          }
        },
        'Member.AssignedToSubscription': (state, data) => {
          if (state[data.subscriptionId]) {
            state[data.subscriptionId].members.push(data.memberId)
          }
        }
      }
    },
    {
      viewId: 'Assignment.Tracker',
      initialState: {},
      reduce: {
        'Members.AssignmentStarted': (state, data) => {
          state[data.subscriptionId] = {
            pending: new Set(data.members),
            completed: [],
            failed: []
          }
        },
        'Member.AssignedToSubscription': (state, data) => {
          state[data.subscriptionId].completed.push(data.memberId)
          state[data.subscriptionId].pending.delete(data.memberId)
        },
        'Failed.ToAssignMemberToSubscription': (state, data) => {
          state[data.subscriptionId].failed.push(data.memberId)
          state[data.subscriptionId].pending.delete(data.memberId)
        }
      }
    },
    {
      viewId: 'Emails.To.Send',
      initialState: {
        nextId: 1,
        list: []
      },
      reduce: {
        'Member.AssignedToSubscription': (state, data) => {
          let notificationId = state.nextId++;
          state.push({
            text: `Hey ${data.memberId}, you were assigned to subscription ${data.subscriptionId}`,
            notificationId,
            attempt: 0
          })
        },
        'Member.AssignmentStarted': (state, data) => {
          let notificationId = state.nextId++;
          state.push({
            text: `Your assignment has been started ${data.memberId}`,
            notificationId,
            attempt: 0
          })
        },
        'Email.Sent': (state, data) => {
          state.list = state.list.filter(x => x.notificationId !== data.notificationId)
        },
        'Email.Failed': (state, data) => {
          state.list.find(x => x.notificationId === data.notificationId).attempt++
        }
      }
    }
  ]

  const StateMachine = [
    {
      viewId: 'Assignment.Tracker',
      trigger: ({ query, produce }) => {
        const trackerView = query(['Assignment.Tracker'])
        for (let subscriptionId in trackerView) {
          const tracker = trackerView[subscriptionId]
          for (let memberId of tracker.pending) {
            produce({
              type: 'Subscription.Assign.Members',
              data: {
                subscriptionId,
                members: [memberId]
              }
            })
          }
        }
      }
    }
  ]

  const FunctionalCore = (StateChange, StateView, StateMachine) => {
    let stateChange = {}
    let stateView = {}
    let stateMachine = {}
    let currentTransaction = []
    StateChange.forEach(sc => stateChange[sc.viewId] = structuredClone(sc.initialState))
    StateView.forEach(sv => stateView[sv.viewId] = structuredClone(sv.initialState))


    const reduce = event => {
      StateView.forEach(sv => {
        const f = sv.reduce[event.type]
        if (f) f(stateView[sv.viewId], event.data)
        // log the views that just changed, chatgpt made a mistake and tried to use the events to trigger the processors
        stateMachine[sv.viewId] = 1;
      })
      StateChange.forEach(sc => {
        const f = sc.reduce?.[event.type]
        if (f) f(stateChange[sc.viewId], event)
      })
      return stateMachine
    }

    // spin the state machines with a consistent snapshot of all the views that just changed (can keep triggering more and more stuff!)
    const trigger = () => {
      let snapshot = structuredClone(stateMachine)
      stateMachine = {}

      StateMachine.forEach(machine => {
        if (snapshot[machine.viewId]) {
          machine.trigger({ query, produce })
        }
      })
    }
    const produce = command => {
      const change = StateChange.find(x => x.viewId === command.type)
      const events = change.map(command.data, stateChange[command.type])
      currentTransaction.push(...events)
      events.forEach(reduce)
      trigger()
      return events
    }

    const consume = event => {
      // always log the external state input for traceability before any state is changed!
      currentTransaction.push(event)
      reduce(event)
      trigger()
    }

    const query = path => (function recurse(state, path) {
      if (path.length === 0) return state
      const key = path[0]
      return state[key] !== undefined ? recurse(state[key], path.slice(1)) : null
    })(stateView, path)

    const commit = () => {
      const tx = currentTransaction
      currentTransaction = []
      return tx
    }

    return { produce, consume, query, reduce, commit }
  }

  return FunctionalCore(StateChange, StateView, StateMachine)

  return core
})()

// ---------- Recovery ----------
const history = await lss.sharedReader.physicalRead()
history.forEach(core.reduce)

// ---------- Txn helper for atomicity between fn core and lss (further make single writer mutex explicit) ----------
let withSingleWriterMutex = (() => {
  let queue = []
  let busy = false;
  return criticalSection => continuation => {
    queue.push([criticalSection, continuation]);
    if (busy === false) {
      busy = true;
      (function update() {
        if (queue.length === 0) {
          busy = false
        } else {
          let [criticalSection, continuation] = queue.shift();
          try {
            // protected access to currentOrderId and we can produce commands/return state inside this function
            // this is equivalent to serializable isolation level in RDBMS without all the nasty multi thread races
            let returnValue = criticalSection()
            let tx = core.commit();
            lss.singleWriter.physicalAppend(tx)
              .then(() => {
                continuation({
                  tx,
                  returnValue
                })
                update()
              })
              .catch(err => {
              // crash the process and go to recovery when we come up, low level IO error in the lss
              console.error(err)
              process.exit(1)
            });
          } catch (err) {
            // crash the process and go to recovery when we come up, reduce threw exception
            // we can optionally support rollback on the state if we use copy instead of in place mutations
            console.error(err)
            process.exit(1)
          }
        }
      })()
      busy = false
    }
  }
})()

// ---------- HTTP API ----------
const routes = {
  POST: {
    "/create-subscription": async (req, res) => {
      let body = ''
      req.on('data', chunk => body += chunk)
      req.on('end', async () => {
        const { plan, createdBy } = JSON.parse(body)
        withSingleWriterMutex(() => {
          core.produce({ type: 'Subscription.Create', data: { plan, createdBy } })
        })(({ txn }) => {
          res.writeHead(200, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ ok: true, subscriptionId: txn[0].data.subscriptionId }))
        })
      })
    }
  },
  GET: {
    "/subscriptions": async (req, res, id) => {
      const result = core.query(['Subscription.List', id])
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify(result))
    }
  }
}

const server = http.createServer(async (req, res) => {
  const methodRoutes = routes[req.method]
  if (!methodRoutes) return res.writeHead(405).end()

  const [path, id] = req.url.split('/').filter(Boolean)
  const routeKey = `/${path}`

  const handler = methodRoutes[routeKey]
  if (!handler) return res.writeHead(404).end()

  handler(req, res, id)
})

// ---------- ExternalStateOutput email sender ----------
if (process.env.SENDGRID_API_KEY) {
  // something catastrophic happened with the network pipe
  let unknown = err => {
    withSingleWriterMutex((action) => {
      core.produce({
        type: 'Email.Failed',
        data: {
          notificationId: action.notificationId,
          actionResult: 'UnknownError',
          message: err.message
        }
      })
    })(() => {})
  }
  setInterval(() => {
    core.query(['Emails.To.Send']).map(action => {
      // action at a distance, its a black box/global singleton, we have no idea whats going on in there
      let req = http.request({
        host: 'api.sendgrid.com',
        path: '/my_send_email_sendpoint',
        headers: {
          Authentication: `Bearer ${process.env.SENDGRID_API_KEY}`,
        }
      }, res => {
        let receiveBuffer = []
        res.on('error', unknown)
        res.on('data', chunk => receiveBuffer.push(chunk))
        res.on('end', () => {
          const actionResult = JSON.parse(Buffer.concat(receiveBuffer).toString())
          withSingleWriterMutex(() => {
            if (res.statusCode === 200) {
              core.produce({
                type: 'Email.Succeeded',
                data: {
                  notificationId: action.notificationId,
                  actionResult
                }
              })
            } else {
              // its something we can fix
              core.produce({
                type: 'Email.Failed',
                data: {
                  notificationId: action.notificationId,
                  actionResult
                }
              })
            }
          })(() => {})
        })
      });
      req.on('error', unknown)
      req.write(Buffer.from(JSON.stringify(action)))
      req.end()
    })
  }, 1000)
}

// ---------- Unit Test Mode (if NODE_ENV=test) ----------
if (process.env.NODE_ENV === 'test') {
  console.log("🧪 Running functional core tests...")

  const plan = "test-plan"
  const createdBy = "ci@test"
  core.produce({ type: 'Subscription.Create', data: { plan, createdBy } })
  const tx = core.commit()

  const assert = (cond, msg) => { if (!cond) throw new Error("❌ " + msg); else console.log("✅", msg) }
  assert(tx.length === 1, "Exactly one Subscription.Created event emitted")
  assert(tx[0].data.plan === plan, "Plan set correctly in event")
  assert(core.query(["Subscription.List", tx[0].data.subscriptionId]).plan === plan, "Plan matches in view")

  console.log("🧪 All tests passed.")
  process.exit(0)
} else {
  server.listen(8080, () => {
    console.log("🧠 Event-sourced API server running at http://localhost:8080")
  })
}
