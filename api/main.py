from fastapi import FastAPI
from api.routes import packages, batches

app = FastAPI(title="MFNY Ops Intelligence API")

app.include_router(packages.router)
app.include_router(batches.router)

@app.get("/")
def root():
    return {"status": "alive"}