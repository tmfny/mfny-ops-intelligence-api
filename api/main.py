from fastapi import FastAPI
from api.routes import packages, batches, runs, ops, inventory, production, compliance, velocity, runs_batches, synthesis, executive, inventory_valuation, qc, distribution, lab, cultivation, sales
app = FastAPI(title="MFNY Ops Intelligence API")

# New BigQuery-backed routers (registered first to take precedence)
app.include_router(inventory.router)
app.include_router(production.router)
app.include_router(compliance.router)
app.include_router(velocity.router)
app.include_router(runs_batches.router)
app.include_router(synthesis.router)
app.include_router(executive.router)
app.include_router(inventory_valuation.router)

# Legacy in-memory routers (will be deprecated as endpoints are migrated)
app.include_router(packages.router)
app.include_router(batches.router)
app.include_router(runs.router)
app.include_router(ops.router)
app.include_router(qc.router)
app.include_router(distribution.router)
app.include_router(lab.router)
app.include_router(cultivation.router)
app.include_router(sales.router)


@app.get("/")
def root():
    return {"status": "alive"}
