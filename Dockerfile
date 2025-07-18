FROM node:22-slim

WORKDIR /app

# Install system dependencies for native modules (including DuckDB)
RUN apt-get update && apt-get install -y \
  python3 \
  make \
  g++ \
  curl \
  build-essential \
  && rm -rf /var/lib/apt/lists/*

# Copy package files and install dependencies
COPY package*.json ./
RUN npm ci --only=production

# Copy the local test file for verification
COPY test-duckdb-simple.js ./

# Test DuckDB installation in container
RUN node test-duckdb-simple.js

# Copy application files
COPY src/ ./src/
COPY public/ ./public/
COPY config.yaml .
COPY .env.example .env

# Create necessary directories with proper permissions
RUN mkdir -p data/raw data/processed data/transformed data/validation data/reports data/database logs \
  && chown -R node:node /app

# Switch to non-root user for security
USER node

# Set environment variables
ENV NODE_ENV=production
ENV PORT=3000

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:3000/health || exit 1

# Run the application
CMD ["npm", "start"]
