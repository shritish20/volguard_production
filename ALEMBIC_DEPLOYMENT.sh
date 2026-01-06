#!/bin/bash
# ALEMBIC_DEPLOYMENT.sh - Complete Alembic setup and deployment

set -e

echo "======================================"
echo "🚀 VolGuard Alembic Setup & Deployment"
echo "======================================"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Step 1: Stop existing containers
echo -e "\n${YELLOW}📦 Stopping existing containers...${NC}"
docker-compose down

# Step 2: Verify Alembic files exist
echo -e "\n${YELLOW}🔍 Verifying Alembic setup...${NC}"

required_files=(
    "alembic/env.py"
    "alembic/script.py.mako"
    "alembic/README"
    "alembic.ini"
)

missing_files=()
for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        missing_files+=("$file")
    fi
done

if [ ${#missing_files[@]} -ne 0 ]; then
    echo -e "${RED}❌ Missing required files:${NC}"
    printf '%s\n' "${missing_files[@]}"
    echo -e "${YELLOW}Please create all files from Step 1 and Step 2${NC}"
    exit 1
fi

if [ ! -d "alembic/versions" ]; then
    echo -e "${YELLOW}📁 Creating alembic/versions directory...${NC}"
    mkdir -p alembic/versions
fi

echo -e "${GREEN}✅ All Alembic files present${NC}"

# Step 3: Verify .env file
echo -e "\n${YELLOW}🔍 Checking environment configuration...${NC}"

if [ ! -f ".env" ]; then
    echo -e "${RED}❌ .env file not found${NC}"
    exit 1
fi

# Load and verify critical variables
source .env

if [ -z "$POSTGRES_PASSWORD" ]; then
    echo -e "${RED}❌ POSTGRES_PASSWORD not set in .env${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Environment variables validated${NC}"

# Step 4: Start database services only
echo -e "\n${YELLOW}🗄️  Starting database services...${NC}"
docker-compose up -d postgres redis

# Wait for PostgreSQL to be ready
echo -e "${YELLOW}⏳ Waiting for PostgreSQL to be ready...${NC}"
max_tries=30
count=0
while [ $count -lt $max_tries ]; do
    if docker-compose exec -T postgres pg_isready -U volguard > /dev/null 2>&1; then
        echo -e "${GREEN}✅ PostgreSQL is ready${NC}"
        break
    fi
    count=$((count + 1))
    echo -n "."
    sleep 1
done

if [ $count -eq $max_tries ]; then
    echo -e "\n${RED}❌ PostgreSQL failed to start${NC}"
    docker-compose logs postgres
    exit 1
fi

# Step 5: Check if database is already initialized
echo -e "\n${YELLOW}🔍 Checking database state...${NC}"

table_count=$(docker-compose exec -T postgres psql -U volguard -d volguard_production -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs || echo "0")

echo -e "${YELLOW}Found $table_count existing tables${NC}"

if [ "$table_count" -gt "0" ]; then
    echo -e "${YELLOW}⚠️  Database already has tables${NC}"
    echo ""
    echo "Choose an option:"
    echo "1. Stamp existing database (keep data, just add Alembic tracking)"
    echo "2. Drop and recreate (DELETES ALL DATA)"
    echo "3. Skip migration setup (assume already configured)"
    echo "0. Exit"
    echo ""
    read -p "Enter choice [0-3]: " db_choice
    
    case $db_choice in
        1)
            echo -e "\n${YELLOW}📌 Stamping existing database...${NC}"
            
            # First, generate initial migration to see what we have
            echo "Generating migration to compare..."
            docker-compose run --rm api alembic revision --autogenerate -m "Initial schema" || true
            
            # Stamp as head
            docker-compose run --rm api alembic stamp head
            
            if [ $? -eq 0 ]; then
                echo -e "${GREEN}✅ Database stamped successfully${NC}"
            else
                echo -e "${RED}❌ Stamping failed${NC}"
                exit 1
            fi
            ;;
        2)
            echo -e "\n${RED}🚨 WARNING: This will DELETE ALL DATA${NC}"
            read -p "Type 'DELETE ALL DATA' to confirm: " confirm
            
            if [ "$confirm" != "DELETE ALL DATA" ]; then
                echo "Cancelled"
                exit 0
            fi
            
            echo -e "${RED}Dropping all tables...${NC}"
            docker-compose exec -T postgres psql -U volguard -d volguard_production -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" > /dev/null 2>&1
            
            echo -e "${GREEN}✅ Database reset${NC}"
            ;;
        3)
            echo -e "${YELLOW}Skipping migration setup${NC}"
            skip_migration=true
            ;;
        0)
            echo "Exiting"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid choice${NC}"
            exit 1
            ;;
    esac
fi

# Step 6: Generate initial migration if needed
if [ -z "$skip_migration" ]; then
    echo -e "\n${YELLOW}🔨 Checking for migrations...${NC}"
    
    migration_count=$(ls -1 alembic/versions/*.py 2>/dev/null | wc -l)
    
    if [ "$migration_count" -eq "0" ]; then
        echo -e "${YELLOW}No migrations found. Generating initial migration...${NC}"
        
        docker-compose run --rm api alembic revision --autogenerate -m "Initial schema"
        
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Migration generation failed${NC}"
            echo "Check logs above for errors"
            exit 1
        fi
        
        echo -e "${GREEN}✅ Initial migration created${NC}"
        
        # Show the generated migration
        echo -e "\n${YELLOW}📄 Generated migration:${NC}"
        ls -lh alembic/versions/
        
        echo -e "\n${YELLOW}⚠️  IMPORTANT: Review the migration file before proceeding${NC}"
        latest_migration=$(ls -t alembic/versions/*.py | head -1)
        echo "File: $latest_migration"
        echo ""
        
        read -p "Press Enter to view migration content, or Ctrl+C to exit..."
        cat "$latest_migration"
        echo ""
        
        read -p "Does this look correct? Continue with migration? (y/N): " continue_migration
        
        if [ "$continue_migration" != "y" ]; then
            echo -e "${YELLOW}Please review and edit the migration file, then run:${NC}"
            echo "docker-compose run --rm api alembic upgrade head"
            exit 0
        fi
    else
        echo -e "${GREEN}✅ Found $migration_count existing migration(s)${NC}"
    fi
    
    # Step 7: Apply migrations
    echo -e "\n${YELLOW}🚀 Applying migrations...${NC}"
    
    docker-compose run --rm api alembic upgrade head
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Migration failed${NC}"
        echo ""
        echo "Troubleshooting steps:"
        echo "1. Check PostgreSQL logs: docker-compose logs postgres"
        echo "2. Verify database connection: docker-compose exec postgres psql -U volguard -d volguard_production -c 'SELECT 1;'"
        echo "3. Check migration file syntax"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Migrations applied successfully${NC}"
fi

# Step 8: Verify tables were created
echo -e "\n${YELLOW}🔍 Verifying database schema...${NC}"

docker-compose exec -T postgres psql -U volguard -d volguard_production -c "\dt" | tee /tmp/volguard_tables.txt

# Check for required tables
required_tables=("trade_records" "decision_journal" "alembic_version")
missing_tables=()

for table in "${required_tables[@]}"; do
    if ! grep -q "$table" /tmp/volguard_tables.txt; then
        missing_tables+=("$table")
    fi
done

if [ ${#missing_tables[@]} -ne 0 ]; then
    echo -e "${RED}❌ Missing required tables:${NC}"
    printf '%s\n' "${missing_tables[@]}"
    exit 1
fi

echo -e "${GREEN}✅ All required tables present${NC}"

# Step 9: Show current migration version
echo -e "\n${YELLOW}📊 Current database version:${NC}"
docker-compose run --rm api alembic current

# Step 10: Run pre-flight checks
echo -e "\n${YELLOW}🔍 Running pre-flight checks...${NC}"

if [ -f "scripts/pre_flight_check.py" ]; then
    python scripts/pre_flight_check.py || true
else
    echo -e "${YELLOW}⚠️  pre_flight_check.py not found, skipping${NC}"
fi

# Step 11: Start all services
echo -e "\n${YELLOW}🚀 Starting all services...${NC}"
docker-compose up -d

echo -e "\n${GREEN}✅ DEPLOYMENT COMPLETE!${NC}"
echo "Use ./scripts/manage_migrations.sh for future updates."
