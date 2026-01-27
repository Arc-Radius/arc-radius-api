from fastapi import FastAPI
from src.routers.bills import router as bills_router
from src.routers.limiter import limiter, limiter_router
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

app = FastAPI()

# Register routers
app.include_router(bills_router)
app.include_router(limiter_router)

# Rate limiting middleware/handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)


@app.get("/")
async def root():
    return {"message": "Server is running!"}
