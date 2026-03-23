from fastapi import FastAPI
from api.routes import packages, batches, runs, overview, ops

app = FastAPI(title="MFNY Ops Intelligence API")

app.include_router(packages.router)
app.include_router(batches.router)
app.include_router(runs.router)
app.include_router(overview.router)
app.include_router(ops.router)