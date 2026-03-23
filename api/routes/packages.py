from fastapi import APIRouter
from api.canix_client import get_packages

router = APIRouter()

@router.get("/packages")
def packages():

    packages = get_packages()

    print("RAW PACKAGES TYPE:", type(packages))
    print("RAW PACKAGES SAMPLE:", str(packages)[:500])
    
    return packages[:50]