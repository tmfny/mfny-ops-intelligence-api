from fastapi import FastAPI
from api.routes import packages, batches, runs, ops

app = FastAPI(title="MFNY Ops Intelligence API")

app.include_router(packages.router)
app.include_router(batches.router)
app.include_router(runs.router)
app.include_router(ops.router)

@app.get("/")
def root():
    return {"status": "alive"}