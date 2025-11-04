#!/usr/bin/env python3
"""
Simple test to check ClickHouse created_at field
"""

import requests
import json
import uuid

def test_clickhouse_created_at():
    """Test ClickHouse created_at field directly"""
    
    print("🔍 Testing ClickHouse created_at field...")
    
    try:
        # Superset API configuration
        superset_url = "http://64.227.129.135:8088"
        access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6dHJ1ZSwiaWF0IjoxNzU5MTM5MzAwLCJqdGkiOiIzMGZkYTJmNS1lMmIxLTQ2ZWYtYjQwNy01YTJiNWE1MjRlZTgiLCJ0eXBlIjoiYWNjZXNzIiwic3ViIjoyNCwibmJmIjoxNzU5MTM5MzAwLCJjc3JmIjoiNDMzMmE5NzMtYTkxMi00MzJlLTkyZjctYTJkOTIyMzljODRjIiwiZXhwIjo0OTEyNzM5MzAwfQ.cQA_bjBCdZGzbnmlo3nl96vxrrIPO0sv-47x6TrDUnY"
        
        # Test query to get created_at field
        sql_query = 'SELECT id, name, created_at FROM "chipchip"."product_names" LIMIT 5'
        
        unique_client_id = f"test_{uuid.uuid4().hex[:6]}"
        
        payload = {
            "client_id": unique_client_id,
            "database_id": 1,
            "json": True,
            "runAsync": False,
            "schema": "chipchip",
            "sql": sql_query,
            "tab": "",
            "expand_data": True
        }
        
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'User-Agent': 'chipchip/bot'
        }
        
        print(f"📊 Testing query: {sql_query}")
        
        response = requests.post(
            f"{superset_url}/api/v1/sqllab/execute/",
            headers=headers,
            json=payload,
            verify=False
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Query executed successfully")
            
            if 'data' in result and result['data']:
                print(f"📋 Sample data:")
                for i, row in enumerate(result['data'][:3], 1):
                    print(f"   {i}. ID: {row[0]}, Name: {row[1]}, Created: {row[2]}")
            else:
                print("⚠️  No data returned")
        else:
            print(f"❌ Query failed with status {response.status_code}: {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_clickhouse_created_at()
