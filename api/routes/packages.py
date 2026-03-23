from fastapi import APIRouter
from api.canix_client import get_packages

router = APIRouter()

@router.get("/packages")
def packages():

    packages = get_packages()

    if not isinstance(packages, list):
        packages = packages.get("data", [])
    
    return packages[:50]