from typing import Union, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db import database, ingredients, clinical_products, dailymed_rxnorms
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

@app.get("/ingredients/", response_model=List[ingredients])
async def get_ingredients(q: Union[str, None] = None):
    if q:
        return await ingredients.objects.filter(name__icontains = q).all()
    return await ingredients.objects.all()

@app.get("/dailymed_rxnorms/", response_model=List[dailymed_rxnorms])
async def get_dailymed_rxnorms(q: Union[str, None] = None, rxcui: Union[str, None] = None):
    if q:
        return await dailymed_rxnorms.objects.filter(rxstr__icontains = q).all()
    if rxcui:
        return await dailymed_rxnorms.objects.filter(rxcui = rxcui).all()
    return await dailymed_rxnorms.objects.all()

@app.get("/clinical_products/", response_model=List[clinical_products])
async def get_clinical_products(q: Union[str, None] = None):
    if q:
        return await clinical_products.objects.filter(name__icontains = q).all()
    return await clinical_products.objects.all()

@app.on_event("startup")
async def startup():
    if not database.is_connected:
        await database.connect()

@app.on_event("shutdown")
async def shutdown():
    if database.is_connected:
        await database.disconnect()
