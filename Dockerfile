FROM mhart/alpine-node AS builder
WORKDIR /app
COPY package.json .
COPY package-lock.json .
RUN npm install
COPY . .
RUN npm run build

FROM mhart/alpine-node AS production
WORKDIR /app
COPY package.json .
COPY package-lock.json .
COPY --from=builder /app/dist ./dist
RUN npm install --production

FROM mhart/alpine-node
WORKDIR /app
COPY package.json .
COPY --from=production /app/node_modules ./node_modules
COPY --from=production /app/dist ./dist

ENV NODE_ENV=production
CMD ["node", "./dist/index.js"]
