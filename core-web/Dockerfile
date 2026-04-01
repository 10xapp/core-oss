FROM node:22-slim

WORKDIR /app

# Install dependencies
COPY package.json package-lock.json ./
RUN npm ci

# Copy only the application code needed to run
COPY index.html vite.config.ts tsconfig.json tsconfig.app.json tsconfig.node.json ./
COPY src/ src/
COPY public/ public/

RUN chown -R node:node /app

USER node

EXPOSE 3000

# Development server with hot reload
CMD ["npm", "run", "dev", "--", "--host"]
