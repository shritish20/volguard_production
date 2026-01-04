#!/usr/bin/env python3
"""
Pre-Flight Check: Validates system before starting supervisor.
Run this BEFORE deploying to catch configuration issues.
"""
import asyncio
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings
from app.core.market.data_client import MarketDataClient, NIFTY_KEY
from app.database import init_db, AsyncSessionLocal
from sqlalchemy import text
import redis.asyncio as redis

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def print_success(msg):
    print(f"{Colors.GREEN}✅ {msg}{Colors.END}")

def print_error(msg):
    print(f"{Colors.RED}❌ {msg}{Colors.END}")

def print_warning(msg):
    print(f"{Colors.YELLOW}⚠️  {msg}{Colors.END}")

def print_info(msg):
    print(f"{Colors.BLUE}ℹ️  {msg}{Colors.END}")

async def check_environment():
    """Validate environment variables"""
    print_info("Checking environment configuration...")

    required_vars = [
        "UPSTOX_ACCESS_TOKEN",
        "POSTGRES_PASSWORD",
        "BASE_CAPITAL",
        "MAX_DAILY_LOSS"
    ]

    missing = []
    for var in required_vars:
        if not getattr(settings, var, None):
            missing.append(var)

    if missing:
        print_error(f"Missing required environment variables: {', '.join(missing)}")
        return False

    print_success("Environment variables validated")

    # Warnings for optional but recommended
    if not settings.TELEGRAM_BOT_TOKEN:
        print_warning("TELEGRAM_BOT_TOKEN not set - alerts will be disabled")

    if settings.ENVIRONMENT == "production_live":
        print_warning("⚠️  ENVIRONMENT=production_live - REAL MONEY MODE")
    else:
        print_success(f"Environment: {settings.ENVIRONMENT}")

    return True

async def check_database():
    """Validate database connection"""
    print_info("Checking database connection...")

    try:
        await init_db()

        async with AsyncSessionLocal() as session:
            result = await session.execute(text("SELECT 1"))
            result.scalar()

        print_success("Database connection successful")

        # Check for required tables
        async with AsyncSessionLocal() as session:
            result = await session.execute(text(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
            ))
            tables = [row[0] for row in result.fetchall()]

        required_tables = ["trade_records", "decision_journal"]
        missing_tables = [t for t in required_tables if t not in tables]

        if missing_tables:
            print_error(f"Missing database tables: {', '.join(missing_tables)}")
            print_info("Run: alembic upgrade head")
            return False

        print_success("Required database tables exist")
        return True

    except Exception as e:
        print_error(f"Database connection failed: {e}")
        return False

async def check_redis():
    """Validate Redis connection"""
    print_info("Checking Redis connection...")

    try:
        r = redis.from_url(settings.REDIS_URL, decode_responses=True)
        await r.ping()
        await r.close()

        print_success("Redis connection successful")
        return True

    except Exception as e:
        print_error(f"Redis connection failed: {e}")
        print_warning("System will run with degraded idempotency protection")
        return True  # Non-critical with our fixes

async def check_upstox_api():
    """Validate Upstox API access"""
    print_info("Checking Upstox API access...")

    try:
        client = MarketDataClient(settings.UPSTOX_ACCESS_TOKEN)

        # Test live quote
        quotes = await client.get_live_quote([NIFTY_KEY])
        if not quotes or NIFTY_KEY not in quotes:
            print_error("Failed to fetch live market data")
            return False

        spot = quotes[NIFTY_KEY]
        print_success(f"Upstox API access verified (NIFTY: {spot})")

        # Test holidays endpoint
        holidays = await client.get_holidays()
        print_success(f"Holiday calendar fetched ({len(holidays)} holidays)")

        await client.close()
        return True

    except Exception as e:
        print_error(f"Upstox API access failed: {e}")
        print_info("Check if token is expired: python scripts/token_manager.py")
        return False

async def check_disk_space():
    """Check available disk space for logs"""
    print_info("Checking disk space...")

    import shutil
    stat = shutil.disk_usage(".")

    free_gb = stat.free / (1024**3)

    if free_gb < 1:
        print_error(f"Low disk space: {free_gb:.2f} GB free")
        return False
    elif free_gb < 5:
        print_warning(f"Limited disk space: {free_gb:.2f} GB free")
    else:
        print_success(f"Disk space: {free_gb:.2f} GB free")

    return True

async def check_log_directories():
    """Ensure log directories exist and are writable"""
    print_info("Checking log directories...")

    dirs = ["logs", "journal"]

    for dir_name in dirs:
        path = Path(dir_name)
        path.mkdir(exist_ok=True)

        # Test write
        test_file = path / ".write_test"
        try:
            test_file.write_text("test")
            test_file.unlink()
            print_success(f"{dir_name}/ directory is writable")
        except Exception as e:
            print_error(f"{dir_name}/ directory not writable: {e}")
            return False

    return True

async def check_market_hours():
    """Check if market is open"""
    print_info("Checking market hours...")

    from datetime import datetime
    import pytz

    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)

    market_open = now.replace(hour=9, minute=15, second=0)
    market_close = now.replace(hour=15, minute=30, second=0)

    if market_open <= now <= market_close:
        print_success("Market is currently OPEN")
    else:
        print_warning("Market is currently CLOSED")
        print_info(f"Current IST time: {now.strftime('%H:%M:%S')}")

    # Check if today is a holiday
    try:
        client = MarketDataClient(settings.UPSTOX_ACCESS_TOKEN)
        holidays = await client.get_holidays()
        await client.close()

        if now.date() in holidays:
            print_warning("Today is a market holiday")
    except:
        pass

    return True

async def run_all_checks():
    """Run all pre-flight checks"""
    print("\n" + "="*60)
    print("🚀 VolGuard Pre-Flight Check")
    print("="*60 + "\n")

    checks = [
        ("Environment", check_environment),
        ("Database", check_database),
        ("Redis", check_redis),
        ("Upstox API", check_upstox_api),
        ("Disk Space", check_disk_space),
        ("Log Directories", check_log_directories),
        ("Market Hours", check_market_hours),
    ]

    results = []

    for name, check_func in checks:
        try:
            result = await check_func()
            results.append((name, result))
        except Exception as e:
            print_error(f"{name} check crashed: {e}")
            results.append((name, False))
        print()  # Blank line between checks

    # Summary
    print("="*60)
    print("📊 Pre-Flight Summary")
    print("="*60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "PASS" if result else "FAIL"
        color = Colors.GREEN if result else Colors.RED
        print(f"{color}{status:6s}{Colors.END} {name}")

    print()
    print(f"Score: {passed}/{total}")

    if passed == total:
        print_success("All checks passed! System is ready for deployment.")
        return 0
    else:
        print_error(f"{total - passed} check(s) failed. Fix issues before deploying.")
        return 1

if __name__ == "__main__":
    load_dotenv()
    exit_code = asyncio.run(run_all_checks())
    sys.exit(exit_code)
