from fastapi import APIRouter
from api.canix_client import get_packages, get_batches, get_runs

router = APIRouter()

@router.get("/ops/overview")
def overview():

    packages = get_packages()
    batches = get_batches()
    runs = get_runs()

    return {
        "packages_count": len(packages),
        "batches_count": len(batches),
        "runs_count": len(runs)
    }