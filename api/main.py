from fastapi import FastAPI
from api.routes import packages, batches, runs, ops

import threading
from api.routes.ops import warm_cache, background_refresh

app = FastAPI(title="MFNY Ops Intelligence API")

app.include_router(packages.router)
app.include_router(batches.router)
app.include_router(runs.router)
app.include_router(ops.router)

@app.on_event("startup")
def start_background_tasks():
    print("🚀 Starting background tasks...")

    threading.Thread(target=warm_cache, daemon=True).start()
    threading.Thread(target=background_refresh, daemon=True).start()

@app.get("/")
def root():
    return {"status": "alive"}