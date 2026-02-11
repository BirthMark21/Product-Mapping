#!/usr/bin/env python3
"""
Quick Setup Script
Validates environment and prepares the system for production use
"""

import os
import sys
import subprocess
from pathlib import Path
from utils.db_connector import get_db_engine

def print_header(text):
    """Print formatted header"""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80)

def check_python_version():
    """Check Python version"""
    print_header("🐍 CHECKING PYTHON VERSION")
    
    version = sys.version_info
    print(f"Python version: {version.major}.{version.minor}.{version.micro}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8+ required")
        return False
    
    print("✅ Python version OK")
    return True

def check_env_file():
    """Check if .env file exists and has required variables"""
    print_header("📝 CHECKING ENVIRONMENT CONFIGURATION")
    
    env_file = Path('.env')
    
    if not env_file.exists():
        print("❌ .env file not found")
        print("   Please create .env file with database credentials")
        return False
    
    print("✅ .env file found")
    
    # Check for required variables
    required_vars = [
        'SUPABASE_PG_HOST',
        'SUPABASE_PG_PASSWORD',
        'PROD_PG_HOST',
        'PROD_PG_PASSWORD',
        'HUB_PG_HOST',
        'HUB_PG_PASSWORD'
    ]
    
    missing_vars = []
    
    with open(env_file, 'r') as f:
        content = f.read()
        for var in required_vars:
            if var not in content:
                missing_vars.append(var)
            elif f'{var}="your_password_here"' in content or f'{var}=""' in content:
                missing_vars.append(f"{var} (not configured)")
    
    if missing_vars:
        print("⚠️  Missing or unconfigured variables:")
        for var in missing_vars:
            print(f"   • {var}")
        return False
    
    print("✅ All required environment variables configured")
    return True

def install_dependencies():
    """Install Python dependencies"""
    print_header("📦 INSTALLING DEPENDENCIES")
    
    requirements_file = Path('requirements.txt')
    
    if not requirements_file.exists():
        print("⚠️  requirements.txt not found - skipping")
        return True
    
    print("Installing dependencies from requirements.txt...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✅ Dependencies installed successfully")
            return True
        else:
            print("❌ Failed to install dependencies")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Error installing dependencies: {e}")
        return False

def create_directories():
    """Create necessary directories"""
    print_header("📁 CREATING DIRECTORIES")
    
    directories = [
        'logs',
        'scripts/dynamic_mapping/mapping_backups'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created: {directory}")
    
    return True

def test_database_connections():
    """Test database connections"""
    print_header("🔌 TESTING DATABASE CONNECTIONS")
    
    try:
        databases = {
            'supabase': 'Supabase PostgreSQL',
            'prod_postgres': 'Production PostgreSQL (chipchip)',
            'hub': 'Local Hub PostgreSQL'
        }
        
        success_count = 0
        
        for db_type, db_name in databases.items():
            try:
                print(f"\nTesting {db_name}...")
                engine = get_db_engine(db_type)
                
                # Test connection
                with engine.connect() as conn:
                    conn.execute("SELECT 1")
                
                print(f"✅ {db_name} - Connected")
                success_count += 1
                
            except Exception as e:
                print(f"❌ {db_name} - Failed: {e}")
        
        if success_count == len(databases):
            print(f"\n✅ All {len(databases)} database connections successful")
            return True
        else:
            print(f"\n⚠️  {success_count}/{len(databases)} database connections successful")
            return False
            
    except ImportError as e:
        print(f"❌ Failed to import db_connector: {e}")
        return False

def initialize_mappings():
    """Initialize dynamic mappings"""
    print_header("🎨 INITIALIZING DYNAMIC MAPPINGS")
    
    mapping_file = Path('scripts/dynamic_mapping/mapping_config.json')
    
    if mapping_file.exists():
        print("✅ Mapping file already exists")
        return True
    
    print("Creating default mappings...")
    
    try:
        result = subprocess.run(
            [sys.executable, 'scripts/dynamic_mapping/dynamic_mapping_manager.py', '--init'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✅ Mappings initialized successfully")
            return True
        else:
            print("❌ Failed to initialize mappings")
            return False
            
    except Exception as e:
        print(f"❌ Error initializing mappings: {e}")
        return False

def run_migrations():
    """Run database migrations"""
    print_header("🗄️  RUNNING DATABASE MIGRATIONS")
    
    print("Do you want to run database migrations now? (y/n): ", end='')
    response = input().strip().lower()
    
    if response != 'y':
        print("⏭️  Skipping migrations (you can run them later)")
        return True
    
    print("\nRunning migrations...")
    
    try:
        result = subprocess.run(
            [sys.executable, 'migrations/run_all_migrations.py'],
            capture_output=True,
            text=True
        )
        
        print(result.stdout)
        
        if result.returncode == 0:
            print("✅ Migrations completed successfully")
            return True
        else:
            print("❌ Migrations failed")
            if result.stderr:
                print(result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Error running migrations: {e}")
        return False

def print_summary(results):
    """Print setup summary"""
    print_header("📊 SETUP SUMMARY")
    
    total = len(results)
    passed = sum(1 for r in results.values() if r)
    
    for step, result in results.items():
        status = "✅" if result else "❌"
        print(f"{status} {step}")
    
    print(f"\n{passed}/{total} checks passed")
    
    if passed == total:
        print("\n🎉 SETUP COMPLETED SUCCESSFULLY!")
        print("\nNext steps:")
        print("1. Review PRODUCTION_README.md for usage instructions")
        print("2. Run: python pipeline/production_etl.py")
        print("3. Set up auto-sync: python scripts/dynamic_mapping/auto_sync_pipeline.py --monitor")
    else:
        print("\n⚠️  SETUP INCOMPLETE")
        print("Please fix the failed checks above before proceeding")

def main():
    """Main setup function"""
    print("\n" + "🚀" * 40)
    print("PRODUCTION-GRADE PRODUCT STANDARDIZATION PLATFORM")
    print("QUICK SETUP SCRIPT")
    print("🚀" * 40)
    
    results = {}
    
    # Run checks
    results["Python Version"] = check_python_version()
    results["Environment Configuration"] = check_env_file()
    results["Create Directories"] = create_directories()
    
    # Optional: Install dependencies
    print("\nDo you want to install Python dependencies? (y/n): ", end='')
    if input().strip().lower() == 'y':
        results["Install Dependencies"] = install_dependencies()
    else:
        print("⏭️  Skipping dependency installation")
        results["Install Dependencies"] = True
    
    # Test connections (only if env is configured)
    if results["Environment Configuration"]:
        results["Database Connections"] = test_database_connections()
    else:
        results["Database Connections"] = False
    
    # Initialize mappings
    results["Initialize Mappings"] = initialize_mappings()
    
    # Run migrations (optional)
    if results["Database Connections"]:
        results["Database Migrations"] = run_migrations()
    else:
        print("\n⏭️  Skipping migrations (database connections failed)")
        results["Database Migrations"] = False
    
    # Print summary
    print_summary(results)
    
    # Exit with appropriate code
    all_passed = all(results.values())
    sys.exit(0 if all_passed else 1)

if __name__ == "__main__":
    main()
