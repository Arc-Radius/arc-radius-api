from fastapi import FastAPI
from src.routers.bills import router as bills_router
from src.routers.generation import router as generation_router
from magnum import Magnum

app = FastAPI()

app.include_router(bills_router)
app.include_router(generation_router)


@app.get("/")
async def root():
    return {"message": "Server is running!"}

handler = Magnum(app)
