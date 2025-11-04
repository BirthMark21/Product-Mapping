#!/usr/bin/env python3
"""
API Client for Dynamic Mapping FastAPI
Test and interact with the API
"""

import requests
import json
from datetime import datetime

class DynamicMappingClient:
    """Client for Dynamic Mapping API"""
    
    def __init__(self, base_url="http://localhost:8000", secret="your-secret-key"):
        self.base_url = base_url
        self.secret = secret
        self.headers = {
            "Authorization": f"Bearer {secret}",
            "Content-Type": "application/json"
        }
    
    def trigger_pipeline(self, force=False, description="API Client Trigger"):
        """Trigger the dynamic mapping pipeline"""
        
        url = f"{self.base_url}/trigger"
        data = {
            "secret": self.secret,
            "force": force,
            "description": description
        }
        
        try:
            response = requests.post(url, json=data, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"success": False, "error": str(e)}
    
    def get_status(self):
        """Get API status"""
        
        url = f"{self.base_url}/status"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}
    
    def health_check(self):
        """Check API health"""
        
        url = f"{self.base_url}/health"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}
    
    def get_info(self):
        """Get API information"""
        
        url = f"{self.base_url}/"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

def main():
    """Main function to test API"""
    
    print("🧪 Testing Dynamic Mapping API")
    print("=" * 40)
    
    # Create client
    client = DynamicMappingClient()
    
    # Test health check
    print("1. Testing health check...")
    health = client.health_check()
    print(f"   Health: {health}")
    
    # Test status
    print("\n2. Testing status...")
    status = client.get_status()
    print(f"   Status: {status}")
    
    # Test info
    print("\n3. Testing info...")
    info = client.get_info()
    print(f"   Info: {info}")
    
    # Test trigger (uncomment to actually trigger pipeline)
    print("\n4. Testing trigger (dry run)...")
    # trigger_result = client.trigger_pipeline(force=True, description="API Test Trigger")
    # print(f"   Trigger Result: {trigger_result}")
    print("   (Uncomment the lines above to actually trigger the pipeline)")
    
    print("\n✅ API testing completed!")

if __name__ == "__main__":
    main()
