from fastapi import APIRouter, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

# Default limiter: apply a conservative global rate limit
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

limiter_router = APIRouter(prefix="/limits", tags=["limits"])


@limiter_router.get("/health")
@limiter.limit("30/minute")
def rate_limit_health(request: Request):
    """Health endpoint with a modest rate limit."""
    return {"status": "ok"}


@limiter_router.get("/demo")
@limiter.limit("5/minute")
def rate_limit_demo(request: Request):
    """Demonstrate rate limiting on a sample endpoint."""
    return {"message": "This endpoint is rate limited to 5 requests per minute."}
