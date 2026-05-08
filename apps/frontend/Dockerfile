FROM node:20-slim

WORKDIR /app

COPY package*.json ./
COPY apps/frontend/package*.json ./apps/frontend/

RUN npm install

COPY apps/frontend ./apps/frontend

WORKDIR /app/apps/frontend

EXPOSE 3000

CMD ["npm", "run", "dev"]
