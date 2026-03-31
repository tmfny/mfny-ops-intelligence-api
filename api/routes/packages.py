from fastapi import APIRouter
from api.canix_client import get_packages

router = APIRouter()

@router.get("/packages")
def packages():

    packages = get_packages()

    # prevent crash on error
    if isinstance(packages, dict) and "error" in packages:
        return packages

    if not isinstance(packages, list):
        packages = packages.get("data", []) or packages.get("results", []) or []

    return packages[:50]