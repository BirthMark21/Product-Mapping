#!/usr/bin/env python3
"""
Script to switch the ETL pipeline to use local database instead of remote Supabase
This will update the .env file to point to your local master-db
"""

import os

def update_env_for_local():
    """Update .env file to use local database instead of remote Supabase"""
    
    print("Switching ETL pipeline to use local database...")
    
    # Read current .env file
    env_file = ".env"
    if not os.path.exists(env_file):
        print("❌ .env file not found!")
        return False
    
    # Read current content
    with open(env_file, 'r') as f:
        content = f.read()
    
    # Update the Supabase/PostgreSQL settings to point to local database
    new_content = content.replace(
        'PG_HOST=aws-0-eu-central-1.pooler.supabase.com',
        'PG_HOST=localhost'
    ).replace(
        'PG_PORT=5432',
        'PG_PORT=5432'
    ).replace(
        'PG_DB_NAME=postgres',
        'PG_DB_NAME=master-db'
    ).replace(
        'PG_USER=postgres.ladquxscytpamcpyyayc',
        'PG_USER=postgres'
    ).replace(
        'PG_PASSWORD=sbp_e5d7e66df3d32a5ef2ea0e20d50a9a95e25a2921',
        'PG_PASSWORD=birthmark21'
    )
    
    # Write updated content
    with open(env_file, 'w') as f:
        f.write(new_content)
    
    print("✅ Updated .env file to use local database:")
    print("   📍 Host: localhost (was: aws-0-eu-central-1.pooler.supabase.com)")
    print("   📍 Database: master-db (was: postgres)")
    print("   📍 User: postgres (was: postgres.ladquxscytpamcpyyayc)")
    print("   📍 Password: birthmark21 (was: Supabase password)")
    
    return True

def main():
    """Main function"""
    print("🔄 Switching ETL pipeline to local database...")
    print("=" * 50)
    
    try:
        if update_env_for_local():
            print("\n✅ Configuration updated successfully!")
            print("🚀 Your ETL pipeline will now use the local database with all your data!")
            print("\n📋 Next steps:")
            print("   1. Run: python main.py")
            print("   2. The pipeline will now process your local data (112,922+ records)")
            print("   3. You should see much larger numbers in the results!")
        else:
            print("❌ Failed to update configuration")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
