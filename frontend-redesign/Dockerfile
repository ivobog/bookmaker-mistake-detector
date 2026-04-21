FROM node:22-alpine

WORKDIR /app/frontend-redesign

COPY frontend-redesign/package.json ./
COPY frontend-redesign/package-lock.json ./
RUN npm install

COPY frontend-redesign ./
COPY shared ../shared

EXPOSE 5174

CMD ["npm", "run", "dev"]
