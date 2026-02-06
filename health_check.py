#!/usr/bin/env python3
"""
System Health Check
Validates the entire production system status
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.db_connector import get_db_engine
from sqlalchemy import text
from pathlib import Path
import json
from datetime import datetime

class HealthChecker:
    """System health checker"""
    
    def __init__(self):
        self.results = {}
        self.warnings = []
        self.errors = []
    
    def check_environment(self):
        """Check environment configuration"""
        print("\n🔍 CHECKING ENVIRONMENT")
        print("=" * 60)
        
        env_file = Path('.env')
        
        if not env_file.exists():
            self.errors.append("❌ .env file not found")
            print("❌ .env file not found")
            return False
        
        print("✅ .env file exists")
        
        # Check for required variables
        required_vars = [
            'SUPABASE_PG_HOST', 'SUPABASE_PG_PASSWORD',
            'PROD_PG_HOST', 'PROD_PG_PASSWORD',
            'HUB_PG_HOST', 'HUB_PG_PASSWORD'
        ]
        
        from dotenv import load_dotenv
        load_dotenv()
        
        missing = []
        for var in required_vars:
            if not os.getenv(var):
                missing.append(var)
        
        if missing:
            self.errors.append(f"❌ Missing environment variables: {', '.join(missing)}")
            print(f"❌ Missing: {', '.join(missing)}")
            return False
        
        print("✅ All required environment variables configured")
        return True
    
    def check_database_connections(self):
        """Check all database connections"""
        print("\n🔌 CHECKING DATABASE CONNECTIONS")
        print("=" * 60)
        
        databases = {
            'supabase': 'Supabase PostgreSQL',
            'prod_postgres': 'Production PostgreSQL (chipchip)',
            'hub': 'Local Hub PostgreSQL'
        }
        
        all_connected = True
        
        for db_type, db_name in databases.items():
            try:
                engine = get_db_engine(db_type)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                print(f"✅ {db_name}")
                self.results[f'db_{db_type}'] = True
            except Exception as e:
                print(f"❌ {db_name}: {str(e)[:50]}...")
                self.errors.append(f"❌ {db_name} connection failed")
                self.results[f'db_{db_type}'] = False
                all_connected = False
        
        return all_connected
    
    def check_canonical_master_tables(self):
        """Check if canonical_products_master exists in all databases"""
        print("\n📊 CHECKING CANONICAL MASTER TABLES")
        print("=" * 60)
        
        databases = {
            'supabase': 'Supabase PostgreSQL',
            'prod_postgres': 'Production PostgreSQL',
            'hub': 'Local Hub'
        }
        
        all_exist = True
        
        for db_type, db_name in databases.items():
            try:
                engine = get_db_engine(db_type)
                with engine.connect() as conn:
                    result = conn.execute(text("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_name = 'canonical_products_master'
                        )
                    """))
                    exists = result.scalar()
                    
                    if exists:
                        # Count records
                        result = conn.execute(text(
                            "SELECT COUNT(*) FROM canonical_products_master"
                        ))
                        count = result.scalar()
                        print(f"✅ {db_name}: {count:,} parent products")
                        self.results[f'canonical_{db_type}'] = count
                    else:
                        print(f"❌ {db_name}: Table not found")
                        self.errors.append(f"❌ canonical_products_master not found in {db_name}")
                        self.results[f'canonical_{db_type}'] = 0
                        all_exist = False
                        
            except Exception as e:
                print(f"❌ {db_name}: {str(e)[:50]}...")
                self.errors.append(f"❌ Failed to check {db_name}")
                all_exist = False
        
        return all_exist
    
    def check_parent_columns(self):
        """Check if parent_product_id columns exist"""
        print("\n🔗 CHECKING PARENT COLUMNS")
        print("=" * 60)
        
        checks = [
            ('supabase', 'products', 'Supabase.products'),
            ('prod_postgres', 'products', 'Production PostgreSQL.products'),
            ('prod_postgres', 'product_names', 'Production PostgreSQL.product_names')
        ]
        
        all_exist = True
        
        for db_type, table_name, display_name in checks:
            try:
                engine = get_db_engine(db_type)
                with engine.connect() as conn:
                    # Check if column exists
                    result = conn.execute(text("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = :table_name 
                            AND column_name = 'parent_product_id'
                        )
                    """), {'table_name': table_name})
                    exists = result.scalar()
                    
                    if exists:
                        # Count records with parent_id
                        result = conn.execute(text(f"""
                            SELECT 
                                COUNT(*) as total,
                                COUNT(parent_product_id) as with_parent
                            FROM {table_name}
                        """))
                        row = result.fetchone()
                        
                        coverage = (row.with_parent / row.total * 100) if row.total > 0 else 0
                        print(f"✅ {display_name}: {row.with_parent:,}/{row.total:,} ({coverage:.1f}%)")
                        
                        if coverage < 80:
                            self.warnings.append(f"⚠️  {display_name} coverage < 80%")
                    else:
                        print(f"❌ {display_name}: Column not found")
                        self.errors.append(f"❌ parent_product_id not found in {display_name}")
                        all_exist = False
                        
            except Exception as e:
                print(f"❌ {display_name}: {str(e)[:50]}...")
                self.errors.append(f"❌ Failed to check {display_name}")
                all_exist = False
        
        return all_exist
    
    def check_mapping_file(self):
        """Check dynamic mapping configuration"""
        print("\n🎨 CHECKING DYNAMIC MAPPINGS")
        print("=" * 60)
        
        mapping_file = Path('scripts/dynamic_mapping/mapping_config.json')
        
        if not mapping_file.exists():
            print("❌ mapping_config.json not found")
            self.errors.append("❌ Mapping file not found")
            return False
        
        try:
            with open(mapping_file, 'r') as f:
                mappings = json.load(f)
            
            parent_count = len(mappings.get('parent_products', {}))
            child_count = sum(
                len(data.get('children', [])) 
                for data in mappings.get('parent_products', {}).values()
            )
            
            print(f"✅ Mapping file exists")
            print(f"   • {parent_count:,} parent products")
            print(f"   • {child_count:,} child products")
            print(f"   • Last updated: {mappings.get('last_updated', 'Unknown')}")
            
            self.results['mapping_parents'] = parent_count
            self.results['mapping_children'] = child_count
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to load mapping file: {e}")
            self.errors.append("❌ Invalid mapping file")
            return False
    
    def check_logs(self):
        """Check if logs directory exists and has recent logs"""
        print("\n📝 CHECKING LOGS")
        print("=" * 60)
        
        logs_dir = Path('logs')
        
        if not logs_dir.exists():
            print("⚠️  logs/ directory not found")
            self.warnings.append("⚠️  No logs directory")
            return True
        
        log_file = logs_dir / 'etl_pipeline.log'
        
        if not log_file.exists():
            print("⚠️  No ETL pipeline logs found")
            self.warnings.append("⚠️  No pipeline logs")
            return True
        
        # Check log file size
        size_mb = log_file.stat().st_size / (1024 * 1024)
        print(f"✅ Pipeline log exists ({size_mb:.2f} MB)")
        
        if size_mb > 100:
            self.warnings.append("⚠️  Log file > 100MB (consider rotation)")
        
        return True
    
    def generate_report(self):
        """Generate health check report"""
        print("\n" + "=" * 80)
        print("📊 HEALTH CHECK REPORT")
        print("=" * 80)
        print(f"Timestamp: {datetime.now().isoformat()}")
        
        # Summary
        total_checks = len(self.results)
        passed_checks = sum(1 for v in self.results.values() if v)
        
        print(f"\n✅ Passed: {passed_checks}/{total_checks} checks")
        
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"   {warning}")
        
        if self.errors:
            print(f"\n❌ ERRORS ({len(self.errors)}):")
            for error in self.errors:
                print(f"   {error}")
        
        # Overall status
        print("\n" + "=" * 80)
        if not self.errors:
            print("🎉 SYSTEM HEALTHY - All critical checks passed!")
            if self.warnings:
                print("⚠️  Some warnings detected - review above")
        else:
            print("❌ SYSTEM UNHEALTHY - Critical errors detected!")
            print("   Please fix the errors above before running pipeline")
        print("=" * 80)
        
        return len(self.errors) == 0
    
    def run(self):
        """Run all health checks"""
        print("\n" + "🏥" * 40)
        print("SYSTEM HEALTH CHECK")
        print("🏥" * 40)
        
        checks = [
            ("Environment", self.check_environment),
            ("Database Connections", self.check_database_connections),
            ("Canonical Master Tables", self.check_canonical_master_tables),
            ("Parent Columns", self.check_parent_columns),
            ("Dynamic Mappings", self.check_mapping_file),
            ("Logs", self.check_logs)
        ]
        
        for check_name, check_func in checks:
            try:
                check_func()
            except Exception as e:
                print(f"\n❌ {check_name} check failed: {e}")
                self.errors.append(f"❌ {check_name} check failed")
        
        # Generate report
        is_healthy = self.generate_report()
        
        return is_healthy


def main():
    """Main entry point"""
    checker = HealthChecker()
    is_healthy = checker.run()
    
    sys.exit(0 if is_healthy else 1)


if __name__ == "__main__":
    main()
