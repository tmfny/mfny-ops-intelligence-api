from fastapi import FastAPI
from api.routes import packages

app = FastAPI(title="MFNY Ops Intelligence API")

app.include_router(packages.router)

@app.get("/")
def root():
    return {"status": "alive"}