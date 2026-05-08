FROM node:20-slim

WORKDIR /app

COPY package*.json ./
COPY apps/backend/package*.json ./apps/backend/

RUN npm install

COPY apps/backend ./apps/backend

WORKDIR /app/apps/backend

EXPOSE 5005

CMD ["npm", "run", "dev"]
