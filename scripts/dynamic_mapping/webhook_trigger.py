#!/usr/bin/env python3
"""
FastAPI-based trigger for dynamic mapping pipeline
Can be triggered by external systems or API calls
"""

import sys
import os
import json
import subprocess
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import uvicorn

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

app = FastAPI(
    title="Dynamic Mapping API",
    description="API for triggering dynamic mapping pipeline",
    version="1.0.0"
)

security = HTTPBearer()

# Pydantic models
class TriggerRequest(BaseModel):
    secret: str
    force: bool = False
    description: str = "API triggered pipeline"

class TriggerResponse(BaseModel):
    success: bool
    message: str
    timestamp: str
    error: str = None

class StatusResponse(BaseModel):
    status: str
    timestamp: str
    version: str

def verify_secret(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify API secret"""
    expected_secret = os.getenv('WEBHOOK_SECRET', 'your-secret-key')
    if credentials.credentials != expected_secret:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid secret key"
        )
    return credentials.credentials

def run_pipeline():
    """Run the complete pipeline"""
    
    print("🚀 FastAPI triggered pipeline")
    print("=" * 40)
    print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Run validation
        result1 = subprocess.run([sys.executable, 'validate_mapping.py'], 
                              capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        if result1.returncode != 0:
            return {"success": False, "error": "Validation failed", "details": result1.stderr}
        
        # Run master table update
        result2 = subprocess.run([sys.executable, 'update_mapping.py'], 
                              capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        if result2.returncode != 0:
            return {"success": False, "error": "Master table update failed", "details": result2.stderr}
        
        # Run remote table update
        result3 = subprocess.run([sys.executable, 'apply_dynamic_mapping.py'], 
                              capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        if result3.returncode != 0:
            return {"success": False, "error": "Remote table update failed", "details": result3.stderr}
        
        return {"success": True, "message": "Pipeline completed successfully"}
        
    except Exception as e:
        return {"success": False, "error": f"Pipeline execution failed: {str(e)}"}

@app.post("/trigger", response_model=TriggerResponse)
async def trigger_pipeline(request: TriggerRequest, secret: str = Depends(verify_secret)):
    """
    Trigger the dynamic mapping pipeline
    
    - **secret**: API secret key for authentication
    - **force**: Force pipeline execution even if no changes detected
    - **description**: Description of the trigger
    """
    
    try:
        print(f"📡 API trigger received: {request.description}")
        
        # Run pipeline
        result = run_pipeline()
        
        if result['success']:
            return TriggerResponse(
                success=True,
                message=result['message'],
                timestamp=datetime.now().isoformat()
            )
        else:
            return TriggerResponse(
                success=False,
                message="Pipeline failed",
                timestamp=datetime.now().isoformat(),
                error=result['error']
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Pipeline trigger failed: {str(e)}"
        )

@app.get("/status", response_model=StatusResponse)
async def get_status():
    """
    Get pipeline status
    
    Returns current status of the API service
    """
    
    return StatusResponse(
        status="running",
        timestamp=datetime.now().isoformat(),
        version="1.0.0"
    )

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    
    Returns service health status
    """
    
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/")
async def root():
    """
    Root endpoint with API information
    """
    
    return {
        "message": "Dynamic Mapping API",
        "version": "1.0.0",
        "endpoints": {
            "trigger": "/trigger",
            "status": "/status", 
            "health": "/health",
            "docs": "/docs"
        }
    }

if __name__ == "__main__":
    print("🌐 Starting FastAPI server...")
    print("📡 API Documentation: http://localhost:8000/docs")
    print("📊 Status endpoint: http://localhost:8000/status")
    print("💚 Health check: http://localhost:8000/health")
    print("🚀 Trigger endpoint: http://localhost:8000/trigger")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
