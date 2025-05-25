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
          VALUES (0, 'Light', 'LetThereBeLight', '{}', $$ {} $$);
        `)
      }
      let currentOrderId = 0
      return {
        physicalAppend: async (events) => {
          const appendTime = new Date().toISOString()
          const values = events.map(({ partitionId, type, data, metadata }) => {
            metadata.appendTime = appendTime
            currentOrderId++
            return `(${currentOrderId}, '${partitionId}', '${type}', $$${JSON.stringify(data).replace(/\$/g, '$')}$$, $$${JSON.stringify(metadata)}$$)`
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
    let currentTransaction = []
    StateChange.forEach(sc => stateChange[sc.viewId] = structuredClone(sc.initialState))
    StateView.forEach(sv => stateView[sv.viewId] = structuredClone(sv.initialState))

    const reduce = event => {
      StateView.forEach(sv => {
        const f = sv.reduce[event.type]
        if (f) f(stateView[sv.viewId], event.data)
      })
      StateChange.forEach(sc => {
        const f = sc.reduce?.[event.type]
        if (f) f(stateChange[sc.viewId], event)
      })
    }

    const produce = command => {
      const change = StateChange.find(x => x.viewId === command.type)
      const events = change.map(command.data, stateChange[command.viewId])
      currentTransaction.push(...events)
      events.forEach(reduce)
      StateMachine.forEach(machine => {
        if (events.some(e => machine.viewId === e.type || machine.viewId === e.data?.viewId)) {
          machine.trigger({ query, produce })
        }
      })
      return events
    }

    const consume = event => {
      reduce(event) // processors don’t run during replay
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

  const core = FunctionalCore(StateChange, StateView, StateMachine)

  // ---------- Test Suite ----------
  const assert = (condition, message) => {
    if (!condition) throw new Error("❌ Test failed: " + message)
    else console.log("✅", message)
  }

  console.log("🧪 Running functional core tests...")

  const plan = "premium"
  const createdBy = "user@example.com"

  core.produce({ type: 'Subscription.Create', data: { plan, createdBy } })
  const tx = core.commit()

  assert(tx.length === 1, "Should produce one Subscription.Created event")
  assert(tx[0].type === 'Subscription.Created', "Event type is Subscription.Created")
  assert(tx[0].data.plan === plan, "Plan is set correctly")
  assert(tx[0].data.createdBy === createdBy, "CreatedBy is set correctly")

  const queryResult = core.query(['Subscription.List', tx[0].data.subscriptionId])
  assert(queryResult !== undefined, "Subscription view was updated")
  assert(queryResult.plan === plan, "Subscription plan in view matches input")
  assert(queryResult.members.length === 0, "No members assigned yet")

  console.log("🧪 All tests passed. Functional core ready.")

  return core
})()

// ---------- Recovery ----------
const history = await lss.sharedReader.physicalRead()
history.forEach(core.reduce)

// ---------- HTTP API ----------
const routes = {
  POST: {
    "/create-subscription": async (req, res) => {
      let body = ''
      req.on('data', chunk => body += chunk)
      req.on('end', async () => {
        const { plan, createdBy } = JSON.parse(body)
        core.produce({ type: 'Subscription.Create', data: { plan, createdBy } })
        const tx = core.commit()
        await lss.singleWriter.physicalAppend(tx.map(e => ({ ...e, partitionId: `subscription-${e.data.subscriptionId}` })))
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ ok: true, subscriptionId: tx[0].data.subscriptionId }))
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

server.listen(8080, () => {
  console.log("🧠 Event-sourced API server running at http://localhost:8080")
})
