from fastapi import FastAPI

from app.db import database
app = FastAPI(title="SageRx")


@app.get("/")
def read_root():
    return {"Welcome": "Hi to SageRx"}


@app.on_event("startup")
async def startup():
    if not database.is_connected:
        await database.connect()
ß
@app.on_event("shutdown")
async def shutdown():
    if database.is_connected:
        await database.disconnect()
