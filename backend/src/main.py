from fastapi import FastAPI
from mangum import Mangum
from src.routers.bills import router as bills_router
from src.routers.generation import router as generation_router

app = FastAPI(
    title="Arc Radius API",
    description="Legislative tracking and advocacy tools for LGBTQ+ youth",
    version="1.0.0",
)

app.include_router(bills_router)
app.include_router(generation_router)


@app.get("/")
async def root():
    return {"status": "ok", "service": "arc-radius-api"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


# Lambda entry point — Mangum translates API Gateway events → FastAPI
handler = Mangum(app, lifespan="off")
