"""
API Server: FastAPI REST API on port 5000
For LLM access to cryptocurrency and equity data
"""
import uvicorn
from api import app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
