from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse
import httpx


router = APIRouter()

PSGC_BASE_URL = "https://psgc.cloud/api"


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
