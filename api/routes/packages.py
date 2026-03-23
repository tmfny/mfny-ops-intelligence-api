from fastapi import APIRouter
from api.canix_client import get_packages

router = APIRouter()

@router.get("/packages")
def packages():

    packages = get_packages()

    # only return first 50 rows
    return packages[:50]