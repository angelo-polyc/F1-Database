"""
Combined server: Streamlit Dashboard + FastAPI on port 5000
Routes /api/* to FastAPI, everything else to Streamlit
"""
import subprocess
import sys
import time
import signal
import os
from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
import httpx
import uvicorn

STREAMLIT_PORT = 5001
API_PORT = 8001

streamlit_proc = None
api_proc = None

def start_services():
    global streamlit_proc, api_proc
    
    streamlit_proc = subprocess.Popen([
        sys.executable, "-m", "streamlit", "run", "dashboard.py",
        "--server.port", str(STREAMLIT_PORT),
        "--server.address", "127.0.0.1",
        "--server.headless", "true"
    ])
    
    api_proc = subprocess.Popen([
        sys.executable, "-m", "uvicorn", "api:app",
        "--host", "127.0.0.1",
        "--port", str(API_PORT)
    ])
    
    time.sleep(3)

def cleanup(signum=None, frame=None):
    if streamlit_proc:
        streamlit_proc.terminate()
    if api_proc:
        api_proc.terminate()
    sys.exit(0)

signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    start_services()

@app.on_event("shutdown")
async def shutdown():
    cleanup()

@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_api(request: Request, path: str):
    async with httpx.AsyncClient() as client:
        url = f"http://127.0.0.1:{API_PORT}/{path}"
        
        headers = dict(request.headers)
        headers.pop("host", None)
        
        body = await request.body()
        
        try:
            resp = await client.request(
                method=request.method,
                url=url,
                headers=headers,
                params=request.query_params,
                content=body,
                timeout=60.0
            )
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=dict(resp.headers)
            )
        except Exception as e:
            return Response(content=str(e), status_code=502)

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
@app.api_route("/", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_streamlit(request: Request, path: str = ""):
    async with httpx.AsyncClient() as client:
        url = f"http://127.0.0.1:{STREAMLIT_PORT}/{path}"
        
        headers = dict(request.headers)
        headers.pop("host", None)
        
        body = await request.body()
        
        try:
            resp = await client.request(
                method=request.method,
                url=url,
                headers=headers,
                params=request.query_params,
                content=body,
                timeout=60.0
            )
            
            resp_headers = dict(resp.headers)
            resp_headers.pop("transfer-encoding", None)
            
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=resp_headers
            )
        except Exception as e:
            return Response(content=str(e), status_code=502)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
