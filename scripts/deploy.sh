#!/bin/bash
set -e

echo "🚀 VolGuard Deployment Script"
echo "=============================="

# Load environment
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "❌ .env file not found"
    exit 1
fi

# Validate critical environment variables
echo ""
echo "🔍 Validating environment..."
if [ -z "$UPSTOX_ACCESS_TOKEN" ]; then
    echo "❌ UPSTOX_ACCESS_TOKEN not set"
    exit 1
fi

if [ -z "$POSTGRES_PASSWORD" ]; then
    echo "❌ POSTGRES_PASSWORD not set"
    exit 1
fi

echo "✅ Environment validated"

# Create required directories
echo ""
echo "📁 Creating directories..."
mkdir -p logs journal data
echo "✅ Directories created"

# Run pre-flight checks
echo ""
echo "🔍 Running pre-flight checks..."
python scripts/pre_flight_check.py
if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Pre-flight checks failed"
    echo "Fix the issues above before deploying"
    exit 1
fi

# Start Docker services
echo ""
echo "🐳 Starting Docker services..."
docker-compose up -d postgres redis

# Wait for services with health checks
echo ""
echo "⏳ Waiting for services to be healthy..."
max_wait=60
elapsed=0

while [ $elapsed -lt $max_wait ]; do
    if docker-compose ps | grep -q "postgres.*healthy" && \
       docker-compose ps | grep -q "redis.*healthy"; then
        echo "✅ Services are healthy"
        break
    fi

    sleep 2
    elapsed=$((elapsed + 2))
    echo -n "."
done

if [ $elapsed -ge $max_wait ]; then
    echo ""
    echo "❌ Services failed to become healthy within ${max_wait}s"
    docker-compose logs
    exit 1
fi

# Run database migrations
echo ""
echo "🗄️  Running database migrations..."
docker-compose run --rm api alembic upgrade head
if [ $? -ne 0 ]; then
    echo "❌ Database migration failed"
    exit 1
fi
echo "✅ Database migrations complete"

# Start API service
echo ""
echo "🌐 Starting API service..."
docker-compose up -d api

# Wait for API to be ready
echo ""
echo "⏳ Waiting for API to be ready..."
max_wait=30
elapsed=0

while [ $elapsed -lt $max_wait ]; do
    if curl -f -s http://localhost:8000/health > /dev/null 2>&1; then
        echo "✅ API is ready"
        break
    fi

    sleep 2
    elapsed=$((elapsed + 2))
    echo -n "."
done

if [ $elapsed -ge $max_wait ]; then
    echo ""
    echo "❌ API failed to start within ${max_wait}s"
    docker-compose logs api
    exit 1
fi

# Verify API endpoints
echo ""
echo "🔍 Verifying API endpoints..."
curl -f -s http://localhost:8000/api/v1/supervisor/status > /dev/null
if [ $? -eq 0 ]; then
    echo "✅ Supervisor status endpoint responding"
else
    echo "⚠️  Supervisor status endpoint not responding (may start later)"
fi

# Start supervisor
echo ""
echo "🧠 Starting supervisor..."
docker-compose up -d supervisor

# Show deployment summary
echo ""
echo "=============================="
echo "✅ Deployment Complete!"
echo "=============================="
echo ""
echo "📊 Service Status:"
docker-compose ps
echo ""
echo "🔗 Important URLs:"
echo "   • API Docs:       http://localhost:8000/docs"
echo "   • Health Check:   http://localhost:8000/health"
echo "   • Metrics:        http://localhost:8000/metrics"
echo "   • Supervisor:     http://localhost:8000/api/v1/supervisor/status"
echo ""
echo "📝 Logs:"
echo "   • View all logs:        docker-compose logs -f"
echo "   • Supervisor logs:      docker-compose logs -f supervisor"
echo "   • API logs:             docker-compose logs -f api"
echo "   • Local logs:           tail -f logs/volguard_$(date +%Y%m%d).log"
echo ""
echo "🛠️  Management:"
echo "   • Stop system:          docker-compose down"
echo "   • Emergency stop:       python scripts/emergency_stop.py"
echo "   • View positions:       curl http://localhost:8000/api/v1/dashboard/analyze"
echo ""

# Display current mode
if [ "$ENVIRONMENT" = "production_live" ]; then
    echo "⚠️  ⚠️  ⚠️  WARNING ⚠️  ⚠️  ⚠️"
    echo "System is running in FULL_AUTO mode with REAL MONEY"
    echo "⚠️  ⚠️  ⚠️  WARNING ⚠️  ⚠️  ⚠️"
else
    echo "✅ System is running in $ENVIRONMENT mode"
fi

echo ""
echo "📖 Next Steps:"
echo "   1. Monitor logs: docker-compose logs -f supervisor"
echo "   2. Watch metrics: watch -n 5 'curl -s http://localhost:8000/metrics | grep volguard'"
echo "   3. Check positions: curl http://localhost:8000/api/v1/supervisor/status"
echo ""
