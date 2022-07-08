from fastapi import FastAPI

from app.db import database, ingredients
app = FastAPI(title="SageRx")


@app.get("/")
def read_root():
    return {"Welcome": "Hi to SageRx"}

@app.get("/ingredients")
async def read_root():
    return await ingredients.objects.all()

@app.on_event("startup")
async def startup():
    if not database.is_connected:
        await database.connect()

@app.on_event("shutdown")
async def shutdown():
    if database.is_connected:
        await database.disconnect()
