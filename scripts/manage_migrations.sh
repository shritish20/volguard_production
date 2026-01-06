#!/bin/bash
# scripts/manage_migrations.sh
# Helper script for managing Alembic migrations

set -e

echo "🗄️  VolGuard Migration Manager"
echo "=============================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Load environment
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo -e "${RED}❌ .env file not found${NC}"
    exit 1
fi

# Function to run alembic in container
run_alembic() {
    docker-compose run --rm api alembic "$@"
}

# Menu
echo ""
echo "Choose an option:"
echo "1. Show current migration version"
echo "2. Show migration history"
echo "3. Create new migration (auto-generate)"
echo "4. Apply all pending migrations"
echo "5. Rollback last migration"
echo "6. Show migration SQL (dry run)"
echo "7. Reset database (DANGEROUS - deletes all data)"
echo "0. Exit"
echo ""

read -p "Enter choice [0-7]: " choice

case $choice in
    1)
        echo ""
        echo -e "${YELLOW}📊 Current Migration Version:${NC}"
        run_alembic current
        ;;
    2)
        echo ""
        echo -e "${YELLOW}📜 Migration History:${NC}"
        run_alembic history --verbose
        ;;
    3)
        read -p "Enter migration message: " message
        if [ -z "$message" ]; then
            echo -e "${RED}❌ Message required${NC}"
            exit 1
        fi
        echo ""
        echo -e "${YELLOW}🔨 Generating migration...${NC}"
        run_alembic revision --autogenerate -m "$message"
        echo ""
        echo -e "${GREEN}✅ Migration created!${NC}"
        echo -e "${YELLOW}⚠️  IMPORTANT: Review the migration file before applying${NC}"
        echo "Location: alembic/versions/"
        ;;
    4)
        echo ""
        echo -e "${YELLOW}⚠️  This will apply all pending migrations${NC}"
        read -p "Continue? (y/N): " confirm
        if [ "$confirm" != "y" ]; then
            echo "Cancelled"
            exit 0
        fi
        echo ""
        echo -e "${YELLOW}🚀 Applying migrations...${NC}"
        run_alembic upgrade head
        echo ""
        echo -e "${GREEN}✅ Migrations applied${NC}"
        run_alembic current
        ;;
    5)
        echo ""
        echo -e "${RED}⚠️  WARNING: This will rollback the last migration${NC}"
        echo "This may result in data loss if columns were dropped"
        read -p "Are you sure? (y/N): " confirm
        if [ "$confirm" != "y" ]; then
            echo "Cancelled"
            exit 0
        fi
        echo ""
        echo -e "${YELLOW}⏪ Rolling back...${NC}"
        run_alembic downgrade -1
        echo ""
        echo -e "${GREEN}✅ Rollback complete${NC}"
        run_alembic current
        ;;
    6)
        echo ""
        echo -e "${YELLOW}📄 Migration SQL (dry run):${NC}"
        run_alembic upgrade head --sql
        ;;
    7)
        echo ""
        echo -e "${RED}🚨 DANGER ZONE 🚨${NC}"
        echo "This will:"
        echo "  1. Drop ALL tables"
        echo "  2. Recreate from migrations"
        echo "  3. ALL DATA WILL BE LOST"
        echo ""
        read -p "Type 'DELETE ALL DATA' to confirm: " confirm
        if [ "$confirm" != "DELETE ALL DATA" ]; then
            echo "Cancelled"
            exit 0
        fi
        echo ""
        echo -e "${RED}Dropping all tables...${NC}"
        docker-compose exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
        echo ""
        echo -e "${YELLOW}Recreating from migrations...${NC}"
        run_alembic upgrade head
        echo ""
        echo -e "${GREEN}✅ Database reset complete${NC}"
        ;;
    0)
        echo "Goodbye!"
        exit 0
        ;;
    *)
        echo -e "${RED}❌ Invalid choice${NC}"
        exit 1
        ;;
esac
