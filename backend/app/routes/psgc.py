from fastapi import APIRouter, Query, Response
from fastapi.responses import JSONResponse
import httpx


router = APIRouter()

PSGC_BASE_URL = "https://psgc.cloud/api"


@router.get("/resolve-location")
async def resolve_location(
    lat: float = Query(..., description="GPS latitude"),
    lng: float = Query(..., description="GPS longitude"),
):
    """
    Resolve GPS coordinates to a PSGC city/municipality code using
    Nominatim reverse geocoding + PSGC city name lookup.
    """
    # Step 1: Reverse geocode with Nominatim
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            nom = await client.get(
                "https://nominatim.openstreetmap.org/reverse",
                params={"lat": lat, "lon": lng, "format": "json", "addressdetails": 1},
                headers={"User-Agent": "ISMS/1.0 (sports management)"},
            )
        if not nom.is_success:
            return JSONResponse({"city_code": None, "city_name": None}, status_code=200)
        nom_data = nom.json()
    except httpx.HTTPError:
        return JSONResponse({"city_code": None, "city_name": None}, status_code=200)

    addr = nom_data.get("address") or {}
    city_name = (
        addr.get("city")
        or addr.get("municipality")
        or addr.get("town")
        or addr.get("village")
        or addr.get("county")
    )
    if not city_name:
        return JSONResponse({"city_code": None, "city_name": None}, status_code=200)

    # Step 2: Search PSGC cities list and find the best match
    try:
        async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
            psgc = await client.get(
                f"{PSGC_BASE_URL}/cities-municipalities",
                headers={"Accept": "application/json"},
            )
        if not psgc.is_success:
            return JSONResponse({"city_code": None, "city_name": city_name}, status_code=200)
        cities = psgc.json()
    except httpx.HTTPError:
        return JSONResponse({"city_code": None, "city_name": city_name}, status_code=200)

    name_lower = city_name.lower()
    match = next(
        (c for c in cities if name_lower in c.get("name", "").lower()
         or c.get("name", "").lower() in name_lower),
        None,
    )
    if not match:
        return JSONResponse({"city_code": None, "city_name": city_name}, status_code=200)

    return {
        "city_code":     match.get("code"),
        "city_name":     match.get("name"),
        "province_code": match.get("provinceCode"),
        "region_code":   match.get("regionCode"),
    }


@router.get("/{path:path}")
async def proxy_psgc(path: str):
    normalized_path = path.strip("/")

    if not normalized_path or ".." in normalized_path or normalized_path.startswith(("http:", "https:")):
        return JSONResponse({"detail": "Invalid PSGC path."}, status_code=400)

    try:
        async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
            upstream = await client.get(
                f"{PSGC_BASE_URL}/{normalized_path}",
                headers={"Accept": "application/json"},
            )
    except httpx.HTTPError:
        return JSONResponse(
            {"detail": "Location service is temporarily unavailable."},
            status_code=503,
        )

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type", "application/json"),
    )
