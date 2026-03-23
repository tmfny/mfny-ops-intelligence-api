from fastapi import APIRouter
from api.canix_client import get_batches

router = APIRouter()

@router.get("/batches")
def batches():
    return get_batches()