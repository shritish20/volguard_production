#!/usr/bin/env python3
"""
COMPREHENSIVE Pre-Flight Check for VolGuard Production Deployment
Validates everything needed for safe production operation.
"""
import asyncio
import sys
import os
import platform
import subprocess
import shutil
import socket
import time
from pathlib import Path
from datetime import datetime, timedelta
import psutil
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY
from app.database import init_db, AsyncSessionLocal, engine
from sqlalchemy import text
import redis.asyncio as redis

class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

class PreFlightCheck:
    """Comprehensive pre-flight validation system"""
    
    def __init__(self):
        self.results = []
        self.start_time = time.time()
        self.critical_failures = 0
        self.warnings = 0
        
    def print_header(self, text):
        """Print section header"""
        print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.BOLD}{Colors.CYAN}{text.center(60)}{Colors.END}")
        print(f"{Colors.BOLD}{Colors.CYAN}{'='*60}{Colors.END}")
    
    def print_success(self, msg):
        """Print success message"""
        print(f"{Colors.GREEN}✅ {msg}{Colors.END}")
        self.results.append(("SUCCESS", msg))
    
    def print_error(self, msg, critical=False):
        """Print error message"""
        if critical:
            print(f"{Colors.RED}❌ CRITICAL: {msg}{Colors.END}")
            self.critical_failures += 1
            self.results.append(("CRITICAL_FAILURE", msg))
        else:
            print(f"{Colors.RED}❌ {msg}{Colors.END}")
            self.results.append(("FAILURE", msg))
    
    def print_warning(self, msg):
        """Print warning message"""
        print(f"{Colors.YELLOW}⚠️  {msg}{Colors.END}")
        self.warnings += 1
        self.results.append(("WARNING", msg))
    
    def print_info(self, msg):
        """Print info message"""
        print(f"{Colors.BLUE}ℹ️  {msg}{Colors.END}")
        self.results.append(("INFO", msg))
    
    async def check_system_resources(self):
        """Check system resources"""
        self.print_header("1. SYSTEM RESOURCES CHECK")
        
        # CPU cores
        cpu_count = os.cpu_count()
        if cpu_count >= 4:
            self.print_success(f"CPU: {cpu_count} cores")
        else:
            self.print_error(f"Insufficient CPU cores: {cpu_count} (need at least 4)", critical=True)
        
        # RAM
        ram_gb = psutil.virtual_memory().total / (1024**3)
        if ram_gb >= 8:
            self.print_success(f"RAM: {ram_gb:.1f} GB")
        elif ram_gb >= 4:
            self.print_warning(f"RAM: {ram_gb:.1f} GB (8+ GB recommended)")
        else:
            self.print_error(f"Insufficient RAM: {ram_gb:.1f} GB (need at least 4 GB)", critical=True)
        
        # Disk space
        disk = shutil.disk_usage(".")
        free_gb = disk.free / (1024**3)
        if free_gb >= 50:
            self.print_success(f"Disk: {free_gb:.1f} GB free")
        elif free_gb >= 10:
            self.print_warning(f"Disk: {free_gb:.1f} GB free (50+ GB recommended for logs)")
        else:
            self.print_error(f"Low disk space: {free_gb:.1f} GB free", critical=True)
        
        # Disk speed (simple write test)
        try:
            test_file = Path("disk_speed_test.tmp")
            start = time.time()
            with open(test_file, "wb") as f:
                f.write(os.urandom(10 * 1024 * 1024))  # 10 MB
            write_time = time.time() - start
            os.remove(test_file)
            
            speed_mbps = 10 / write_time
            if speed_mbps >= 50:
                self.print_success(f"Disk speed: {speed_mbps:.1f} MB/s")
            elif speed_mbps >= 10:
                self.print_warning(f"Disk speed: {speed_mbps:.1f} MB/s (50+ MB/s recommended for journaling)")
            else:
                self.print_error(f"Slow disk: {speed_mbps:.1f} MB/s", critical=False)
        except Exception as e:
            self.print_warning(f"Disk speed test failed: {e}")
        
        # CPU performance (quick benchmark)
        try:
            start = time.time()
            # Simple matrix multiplication
            a = np.random.rand(1000, 1000)
            b = np.random.rand(1000, 1000)
            np.dot(a, b)
            cpu_time = time.time() - start
            
            if cpu_time < 5:
                self.print_success(f"CPU performance: {cpu_time:.2f}s for 1k×1k matmul")
            elif cpu_time < 10:
                self.print_warning(f"CPU performance: {cpu_time:.2f}s for 1k×1k matmul (slow)")
            else:
                self.print_error(f"Very slow CPU: {cpu_time:.2f}s for 1k×1k matmul", critical=False)
        except Exception as e:
            self.print_warning(f"CPU benchmark failed: {e}")
    
    async def check_environment(self):
        """Validate environment configuration"""
        self.print_header("2. ENVIRONMENT CONFIGURATION")
        
        # Check .env file exists
        env_file = Path(".env")
        if env_file.exists():
            self.print_success(".env file found")
        else:
            self.print_error(".env file not found", critical=True)
            return False
        
        # Required variables
        required_vars = [
            "UPSTOX_ACCESS_TOKEN",
            "POSTGRES_PASSWORD",
            "BASE_CAPITAL",
            "MAX_DAILY_LOSS",
            "ENVIRONMENT"
        ]
        
        missing = []
        for var in required_vars:
            if not getattr(settings, var, None):
                missing.append(var)
        
        if missing:
            self.print_error(f"Missing required environment variables: {', '.join(missing)}", critical=True)
            return False
        
        self.print_success("All required environment variables present")
        
        # Validate values
        try:
            base_capital = float(settings.BASE_CAPITAL)
            if base_capital <= 0:
                self.print_error(f"Invalid BASE_CAPITAL: {base_capital}", critical=True)
            elif base_capital < 100000:
                self.print_warning(f"Low BASE_CAPITAL: {base_capital:,.0f} (recommended: 1,000,000+)")
            else:
                self.print_success(f"BASE_CAPITAL: ₹{base_capital:,.0f}")
        except:
            self.print_error("Invalid BASE_CAPITAL format", critical=True)
        
        try:
            max_daily_loss = float(settings.MAX_DAILY_LOSS)
            if max_daily_loss <= 0:
                self.print_error(f"Invalid MAX_DAILY_LOSS: {max_daily_loss}", critical=True)
            elif max_daily_loss > settings.BASE_CAPITAL * 0.05:
                self.print_warning(f"High MAX_DAILY_LOSS: {max_daily_loss:,.0f} (>5% of capital)")
            else:
                self.print_success(f"MAX_DAILY_LOSS: ₹{max_daily_loss:,.0f}")
        except:
            self.print_error("Invalid MAX_DAILY_LOSS format", critical=True)
        
        # Environment mode
        env = settings.ENVIRONMENT
        if env == "production_live":
            self.print_warning(f"ENVIRONMENT: {env} - REAL MONEY MODE")
        elif env == "production_semi":
            self.print_info(f"ENVIRONMENT: {env} - SEMI-AUTO MODE")
        elif env == "shadow":
            self.print_success(f"ENVIRONMENT: {env} - SHADOW MODE (SAFE)")
        else:
            self.print_warning(f"ENVIRONMENT: {env} - Unknown mode")
        
        # Optional but recommended
        if not settings.TELEGRAM_BOT_TOKEN:
            self.print_warning("TELEGRAM_BOT_TOKEN not set - alerts disabled")
        else:
            self.print_success("Telegram alerts configured")
        
        if not settings.ADMIN_SECRET:
            self.print_warning("ADMIN_SECRET not set - admin API disabled")
        else:
            self.print_success("Admin API configured")
        
        return True
    
    async def check_network(self):
        """Check network connectivity and latency"""
        self.print_header("3. NETWORK CONNECTIVITY")
        
        # Check internet connectivity
        try:
            socket.create_connection(("8.8.8.8", 53), timeout=5)
            self.print_success("Internet connectivity: OK")
        except:
            self.print_error("No internet connectivity", critical=True)
        
        # Check Upstox API endpoints
        endpoints = [
            ("Upstox API V2", "https://api.upstox.com/v2"),
            ("Upstox API V3", "https://api.upstox.com/v3"),
            ("Upstox WebSocket", "wss://api.upstox.com/v2/feed/market-data-feed"),
        ]
        
        for name, url in endpoints:
            try:
                host = url.split("//")[1].split("/")[0]
                port = 443 if url.startswith("https") else 80
                
                start = time.time()
                sock = socket.create_connection((host, port), timeout=5)
                sock.close()
                latency = (time.time() - start) * 1000
                
                if latency < 100:
                    self.print_success(f"{name}: {latency:.0f}ms latency")
                elif latency < 300:
                    self.print_warning(f"{name}: {latency:.0f}ms latency (high)")
                else:
                    self.print_error(f"{name}: {latency:.0f}ms latency (very high)", critical=False)
                    
            except Exception as e:
                self.print_error(f"{name}: Cannot connect - {e}", critical=True)
        
        # Check local services
        try:
            sock = socket.create_connection(("localhost", 5432), timeout=2)
            sock.close()
            self.print_success("PostgreSQL port 5432: Listening")
        except:
            self.print_warning("PostgreSQL port 5432: Not listening (may start via Docker)")
        
        try:
            sock = socket.create_connection(("localhost", 6379), timeout=2)
            sock.close()
            self.print_success("Redis port 6379: Listening")
        except:
            self.print_warning("Redis port 6379: Not listening (may start via Docker)")
        
        try:
            sock = socket.create_connection(("localhost", 8000), timeout=2)
            sock.close()
            self.print_warning("Port 8000: Already in use (may conflict with API)")
        except:
            self.print_success("Port 8000: Available")
    
    async def check_dependencies(self):
        """Check Python dependencies and versions"""
        self.print_header("4. DEPENDENCIES & VERSIONS")
        
        # Python version
        python_version = platform.python_version()
        required_version = (3, 11, 0)
        current_version = tuple(map(int, python_version.split('.')[:3]))
        
        if current_version >= required_version:
            self.print_success(f"Python {python_version} (>=3.11.0 required)")
        else:
            self.print_error(f"Python {python_version} (3.11.0+ required)", critical=True)
        
        # Check critical packages
        critical_packages = [
            ("fastapi", "0.104.1"),
            ("uvicorn", "0.24.0"),
            ("sqlalchemy", "2.0.0"),
            ("pandas", "2.0.0"),
            ("numpy", "1.24.0"),
            ("redis", "5.0.0"),
        ]
        
        for package, min_version in critical_packages:
            try:
                module = __import__(package)
                version = getattr(module, '__version__', 'unknown')
                
                # Simple version check
                if version >= min_version:
                    self.print_success(f"{package}: {version}")
                else:
                    self.print_error(f"{package}: {version} (<{min_version})", critical=True)
                    
            except ImportError:
                self.print_error(f"{package}: Not installed", critical=True)
            except Exception as e:
                self.print_warning(f"{package}: Version check failed - {e}")
        
        # Check if we can import all app modules
        try:
            from app.config import settings
            from app.core.market.data_client import MarketDataClient
            from app.core.trading.executor import TradeExecutor
            from app.lifecycle.supervisor import ProductionTradingSupervisor
            self.print_success("All application modules import successfully")
        except Exception as e:
            self.print_error(f"Application import failed: {e}", critical=True)
    
    async def check_database(self):
        """Validate database configuration and connection"""
        self.print_header("5. DATABASE CONFIGURATION")
        
        try:
            # Initialize database
            await init_db()
            self.print_success("Database initialization: OK")
            
            # Test connection
            async with AsyncSessionLocal() as session:
                result = await session.execute(text("SELECT 1"))
                val = result.scalar()
                if val == 1:
                    self.print_success("Database connection: OK")
                else:
                    self.print_error("Database connection test failed", critical=True)
            
            # Check for required tables
            async with AsyncSessionLocal() as session:
                result = await session.execute(text(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                ))
                tables = [row[0] for row in result.fetchall()]
            
            required_tables = ["trade_records", "decision_journal", "historical_candles", "approval_requests"]
            missing_tables = [t for t in required_tables if t not in tables]
            
            if missing_tables:
                self.print_error(f"Missing database tables: {', '.join(missing_tables)}", critical=True)
                self.print_info("Run: docker-compose run --rm api alembic upgrade head")
            else:
                self.print_success("All required database tables exist")
            
            # Check table sizes (for existing data)
            async with AsyncSessionLocal() as session:
                result = await session.execute(text(
                    "SELECT tablename, pg_size_pretty(pg_total_relation_size('\"' || tablename || '\"')) as size "
                    "FROM pg_tables WHERE schemaname = 'public'"
                ))
                for row in result.fetchall():
                    self.print_info(f"  {row[0]}: {row[1]}")
            
        except Exception as e:
            self.print_error(f"Database check failed: {e}", critical=True)
    
    async def check_redis(self):
        """Validate Redis configuration and connection"""
        self.print_header("6. REDIS CONFIGURATION")
        
        try:
            # Test connection
            r = redis.from_url(settings.REDIS_URL, decode_responses=True)
            await r.ping()
            self.print_success("Redis connection: OK")
            
            # Test basic operations
            test_key = "preflight_test"
            test_value = str(time.time())
            
            await r.set(test_key, test_value, ex=10)
            retrieved = await r.get(test_key)
            
            if retrieved == test_value:
                self.print_success("Redis read/write: OK")
            else:
                self.print_error("Redis read/write test failed", critical=True)
            
            # Check memory usage
            info = await r.info("memory")
            used_memory = int(info.get("used_memory", 0))
            max_memory = int(info.get("maxmemory", 0))
            
            if max_memory > 0:
                memory_percent = (used_memory / max_memory) * 100
                if memory_percent < 80:
                    self.print_success(f"Redis memory: {memory_percent:.1f}% used")
                else:
                    self.print_warning(f"Redis memory: {memory_percent:.1f}% used (high)")
            else:
                self.print_info(f"Redis memory used: {used_memory / 1024 / 1024:.1f} MB")
            
            await r.close()
            
        except redis.ConnectionError as e:
            self.print_error(f"Redis connection failed: {e}", critical=True)
        except Exception as e:
            self.print_error(f"Redis check failed: {e}", critical=True)
    
    async def check_upstox_api(self):
        """Validate Upstox API access"""
        self.print_header("7. UPSTOX API ACCESS")
        
        try:
            client = MarketDataClient(settings.UPSTOX_ACCESS_TOKEN)
            
            # Test live quote
            quotes = await client.get_live_quote([NIFTY_KEY])
            if not quotes or NIFTY_KEY not in quotes:
                self.print_error("Failed to fetch live market data", critical=True)
            else:
                spot = quotes[NIFTY_KEY]
                if spot > 0:
                    self.print_success(f"Upstox API access: OK (NIFTY: {spot})")
                else:
                    self.print_error(f"Invalid NIFTY price: {spot}", critical=True)
            
            # Test holidays endpoint
            holidays = await client.get_holidays()
            if isinstance(holidays, list):
                self.print_success(f"Holiday calendar: {len(holidays)} holidays loaded")
                
                # Check if today is a holiday
                today = datetime.now().date()
                if today in holidays:
                    self.print_error(f"Today ({today}) is a market holiday", critical=True)
                else:
                    self.print_success(f"Today ({today}) is a trading day")
            else:
                self.print_warning("Could not fetch holiday calendar")
            
            # Test contract details
            contract = await client.get_contract_details("NIFTY")
            if contract:
                lot_size = contract.get("lot_size", 0)
                if lot_size > 0:
                    self.print_success(f"NIFTY lot size: {lot_size}")
                else:
                    self.print_warning("Could not determine NIFTY lot size")
            else:
                self.print_warning("Could not fetch contract details")
            
            await client.close()
            
        except Exception as e:
            self.print_error(f"Upstox API access failed: {e}", critical=True)
            self.print_info("Check if token is expired: python scripts/token_manager.py")
    
    async def check_filesystem(self):
        """Check filesystem permissions and structure"""
        self.print_header("8. FILESYSTEM & PERMISSIONS")
        
        required_dirs = ["logs", "journal", "data"]
        
        for dir_name in required_dirs:
            path = Path(dir_name)
            path.mkdir(exist_ok=True)
            
            # Check write permission
            test_file = path / ".write_test"
            try:
                test_file.write_text("test")
                test_file.unlink()
                self.print_success(f"{dir_name}/: Writable")
            except Exception as e:
                self.print_error(f"{dir_name}/: Not writable - {e}", critical=True)
            
            # Check available inodes (for log rotation)
            stat = os.statvfs(path) if hasattr(os, 'statvfs') else None
            if stat:
                free_inodes = stat.f_favail
                if free_inodes < 1000:
                    self.print_warning(f"{dir_name}/: Low inodes ({free_inodes})")
        
        # Check log file permissions
        log_files = ["logs/volguard.log", "logs/volguard_errors.log"]
        for log_file in log_files:
            path = Path(log_file)
            if path.exists():
                try:
                    with open(path, "a") as f:
                        f.write("test\n")
                    self.print_success(f"{log_file}: Appendable")
                except Exception as e:
                    self.print_warning(f"{log_file}: Cannot append - {e}")
        
        # Check journal directory
        journal_path = Path("journal")
        if journal_path.exists():
            journal_files = list(journal_path.glob("*.json"))
            if journal_files:
                self.print_info(f"Journal: {len(journal_files)} existing journal files")
            else:
                self.print_info("Journal: No existing journal files")
        else:
            self.print_success("Journal directory created")
    
    async def check_market_hours(self):
        """Check if market is open"""
        self.print_header("9. MARKET HOURS CHECK")
        
        from datetime import datetime
        import pytz
        
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)
        
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        if market_open <= now <= market_close:
            self.print_success(f"Market is OPEN (Current IST: {now.strftime('%H:%M:%S')})")
            
            # Check if within first/last 15 minutes (volatile)
            if (now - market_open).total_seconds() < 900:
                self.print_warning("Market just opened (first 15 mins - high volatility)")
            elif (market_close - now).total_seconds() < 900:
                self.print_warning("Market closing soon (last 15 mins - high volatility)")
                
        else:
            self.print_warning(f"Market is CLOSED (Current IST: {now.strftime('%H:%M:%S')})")
            
            # Show when it opens next
            if now < market_open:
                hours_until = (market_open - now).total_seconds() / 3600
                self.print_info(f"Market opens in {hours_until:.1f} hours")
            else:
                # Opens tomorrow
                tomorrow = now + timedelta(days=1)
                tomorrow_open = tomorrow.replace(hour=9, minute=15, second=0, microsecond=0)
                hours_until = (tomorrow_open - now).total_seconds() / 3600
                self.print_info(f"Market opens tomorrow in {hours_until:.1f} hours")
    
    async def check_docker(self):
        """Check Docker availability"""
        self.print_header("10. DOCKER & CONTAINERS")
        
        try:
            # Check Docker installation
            result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
            if result.returncode == 0:
                self.print_success(f"Docker: {result.stdout.strip()}")
            else:
                self.print_error("Docker not installed or not in PATH", critical=True)
                return
            
            # Check Docker Compose
            result = subprocess.run(["docker-compose", "--version"], capture_output=True, text=True)
            if result.returncode == 0:
                self.print_success(f"Docker Compose: {result.stdout.strip()}")
            else:
                self.print_warning("Docker Compose not found (docker compose may be available)")
            
            # Check if Docker daemon is running
            result = subprocess.run(["docker", "info"], capture_output=True, text=True)
            if result.returncode == 0:
                self.print_success("Docker daemon: Running")
            else:
                self.print_error("Docker daemon not running", critical=True)
            
            # Check for existing VolGuard containers
            result = subprocess.run(
                ["docker", "ps", "-a", "--filter", "name=volguard", "--format", "{{.Names}}"],
                capture_output=True, text=True
            )
            existing_containers = [c for c in result.stdout.strip().split('\n') if c]
            
            if existing_containers:
                self.print_warning(f"Existing VolGuard containers: {', '.join(existing_containers)}")
                self.print_info("Run: docker-compose down (to clean up)")
            else:
                self.print_success("No existing VolGuard containers")
                
        except FileNotFoundError as e:
            self.print_error(f"Docker check failed: {e}", critical=True)
        except Exception as e:
            self.print_warning(f"Docker check incomplete: {e}")
    
    async def run_all_checks(self):
        """Run all pre-flight checks"""
        self.print_header("🚀 VOLGUARD PRE-FLIGHT CHECK")
        print(f"{Colors.BOLD}Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.END}")
        print(f"{Colors.BOLD}Environment: {os.getenv('ENVIRONMENT', 'unknown')}{Colors.END}")
        print()
        
        checks = [
            ("System Resources", self.check_system_resources),
            ("Environment", self.check_environment),
            ("Network", self.check_network),
            ("Dependencies", self.check_dependencies),
            ("Database", self.check_database),
            ("Redis", self.check_redis),
            ("Upstox API", self.check_upstox_api),
            ("Filesystem", self.check_filesystem),
            ("Market Hours", self.check_market_hours),
            ("Docker", self.check_docker),
        ]
        
        for name, check_func in checks:
            try:
                await check_func()
            except Exception as e:
                self.print_error(f"{name} check crashed: {e}", critical=True)
            print()
        
        # Summary
        elapsed = time.time() - self.start_time
        self.print_header("📊 PRE-FLIGHT SUMMARY")
        
        print(f"{Colors.BOLD}Checks completed in {elapsed:.1f} seconds{Colors.END}")
        print()
        
        # Count results
        success_count = sum(1 for status, _ in self.results if status == "SUCCESS")
        failure_count = sum(1 for status, _ in self.results if status == "FAILURE")
        critical_count = sum(1 for status, _ in self.results if status == "CRITICAL_FAILURE")
        warning_count = sum(1 for status, _ in self.results if status == "WARNING")
        
        print(f"{Colors.GREEN}✅ Successes: {success_count}{Colors.END}")
        print(f"{Colors.YELLOW}⚠️  Warnings: {warning_count}{Colors.END}")
        print(f"{Colors.RED}❌ Failures: {failure_count}{Colors.END}")
        print(f"{Colors.RED}{Colors.BOLD}💀 Critical: {critical_count}{Colors.END}")
        print()
        
        # Show critical failures first
        if critical_count > 0:
            print(f"{Colors.BOLD}{Colors.RED}CRITICAL FAILURES:{Colors.END}")
            for status, msg in self.results:
                if status == "CRITICAL_FAILURE":
                    print(f"  • {msg}")
            print()
        
        # Show warnings
        if warning_count > 0:
            print(f"{Colors.BOLD}{Colors.Yellow}WARNINGS:{Colors.END}")
            for status, msg in self.results:
                if status == "WARNING":
                    print(f"  • {msg}")
            print()
        
        # Recommendations
        if critical_count == 0 and failure_count == 0:
            print(f"{Colors.BOLD}{Colors.GREEN}🎉 ALL CHECKS PASSED!{Colors.END}")
            print("System is ready for deployment.")
            print()
            print(f"{Colors.BOLD}Next steps:{Colors.END}")
            print("  1. Run: ./scripts/deploy.sh")
            print("  2. Monitor: ./scripts/monitor.sh")
            print("  3. Check logs: tail -f logs/volguard_$(date +%Y%m%d).log")
            return 0
        elif critical_count == 0:
            print(f"{Colors.BOLD}{Colors.YELLOW}⚠️  READY WITH WARNINGS{Colors.END}")
            print("System can be deployed, but review warnings above.")
            print()
            print(f"{Colors.BOLD}Next steps:{Colors.END}")
            print("  1. Review and fix warnings if possible")
            print("  2. Run: ./scripts/deploy.sh")
            print("  3. Monitor closely during initial deployment")
            return 1
        else:
            print(f"{Colors.BOLD}{Colors.RED}❌ DEPLOYMENT BLOCKED{Colors.END}")
            print("Critical failures must be fixed before deployment.")
            print()
            print(f"{Colors.BOLD}Immediate actions:{Colors.END}")
            print("  1. Fix all critical failures above")
            print("  2. Re-run: python scripts/pre_flight_check.py")
            print("  3. Do not deploy until all critical checks pass")
            return 2

async def main():
    """Main entry point"""
    # Load environment
    load_dotenv()
    
    # Check if running as root (not recommended)
    if os.geteuid() == 0:
        print(f"{Colors.RED}Warning: Running as root is not recommended{Colors.END}")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Exiting.")
            return 1
    
    # Run checks
    checker = PreFlightCheck()
    return await checker.run_all_checks()

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
