FROM node:22-alpine

WORKDIR /app/frontend

COPY frontend/package.json ./
COPY frontend/package-lock.json ./
RUN npm install

COPY frontend ./
COPY shared ../shared

EXPOSE 5173

CMD ["npm", "run", "dev"]
