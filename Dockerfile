FROM node:18
WORKDIR /app
COPY . .
RUN npm install pg
CMD ["node", "app.js"]
