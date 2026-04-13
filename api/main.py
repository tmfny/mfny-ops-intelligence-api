from fastapi import FastAPI
from api.routes import packages, batches, runs

app = FastAPI(title="MFNY Ops Intelligence API")

app.include_router(packages.router)
app.include_router(batches.router)
app.include_router(runs.router)

@app.get("/")
def root():
    return {"status": "alive"}