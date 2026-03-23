from fastapi import APIRouter
from api.canix_client import get_runs

router = APIRouter()

@router.get("/runs")
def runs():

    runs = get_runs(limit=100)

    return runs[:50]