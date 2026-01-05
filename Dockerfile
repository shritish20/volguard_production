# Dockerfile - Optimized for Production

FROM python:3.11-slim

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install System Dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ✅ OPTIMIZATION: Copy requirements first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ✅ OPTIMIZATION: Copy only necessary files (use .dockerignore)
COPY app/ ./app/
COPY scripts/ ./scripts/
COPY alembic/ ./alembic/
COPY alembic.ini .
COPY run_supervisor.py .
COPY run_production.py .

# Create required directories
RUN mkdir -p logs journal state

# Expose API port
EXPOSE 8000

# ✅ CRITICAL FIX: Default CMD (overridden by docker-compose)
# This is just a fallback - docker-compose.yml specifies the actual command
CMD ["python", "run_supervisor.py"]
