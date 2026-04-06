import logging
from contextlib import asynccontextmanager

logging.getLogger("neo4j").setLevel(logging.ERROR)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from src.core.settings import settings
from src.db.neo4j_client import close_async_driver, get_async_driver
from src.routers.bills import router as bills_router
from src.routers.generation import router as generation_router
from src.routers.states import router as states_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_async_driver()
    yield
    await close_async_driver()


app = FastAPI(
    title="Arc Radius API",
    description="Legislative tracking and advocacy tools for LGBTQ+ youth",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(bills_router)
if settings.rag_and_generation_enabled:
    app.include_router(generation_router)
app.include_router(states_router)


@app.get("/")
async def root():
    return {"status": "ok", "service": "arc-radius-api"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


# Lambda entry point — Mangum translates API Gateway events → FastAPI
handler = Mangum(app, lifespan="off")
