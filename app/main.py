from typing import Union

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db import database, ingredients
app = FastAPI(title="SageRx")

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"Welcome": "Hi to SageRx"}

@app.get("/ingredients")
async def read_root(q: Union[str, None] = None):
    if q:
        return await ingredients.objects.filter(ingredient_name__icontains = q).all()
    return await ingredients.objects.all()

@app.on_event("startup")
async def startup():
    if not database.is_connected:
        await database.connect()

@app.on_event("shutdown")
async def shutdown():
    if database.is_connected:
        await database.disconnect()
