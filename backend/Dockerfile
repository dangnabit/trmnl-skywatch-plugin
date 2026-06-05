FROM node:20-bullseye-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY package.json tsconfig.json ./
RUN npm install

COPY . .
RUN npm run build

ENV ENABLE_IP_WHITELIST=false
ENV REDIS_URL=redis://redis:6379

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

CMD ["node", "dist/app.js"]
