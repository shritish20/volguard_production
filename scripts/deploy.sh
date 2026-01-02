#!/bin/bash

set -e

echo "🚀 Deploying VolGuard Trading System..."

# Load environment
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "❌ .env file not found"
    exit 1
fi

# Validate environment
if [ -z "$UPSTOX_ACCESS_TOKEN" ]; then
    echo "❌ UPSTOX_ACCESS_TOKEN not set"
    exit 1
fi

if [ -z "$POSTGRES_PASSWORD" ]; then
    echo "❌ POSTGRES_PASSWORD not set"
    exit 1
fi

# Create directories
mkdir -p logs journal

# Start services
echo "Starting Docker services..."
docker-compose up -d

# Wait for services
echo "Waiting for services to be ready..."
sleep 30

# Run database migrations
echo "Running database migrations..."
docker-compose exec api alembic upgrade head

# Validate deployment
echo "Validating deployment..."
python validate_deployment.py --phase SHADOW

echo "✅ Deployment complete!"
echo ""
echo "📊 Dashboard: http://localhost:8000/docs"
echo "📝 Logs: tail -f logs/production_supervisor.log"
echo "📈 Supervisor status: curl http://localhost:8000/api/v1/supervisor/status"
echo ""
echo "⚠️  IMPORTANT: Run in SHADOW mode for 7 days before SEMI_AUTO"
