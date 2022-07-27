import databases
import ormar
import sqlalchemy

from typing import Union, Optional, Dict, List
from .config import settings

database = databases.Database(settings.db_url)
metadata = sqlalchemy.MetaData()


class BaseMeta(ormar.ModelMeta):
    metadata = metadata
    database = database


class ingredients(ormar.Model):
    class Meta(BaseMeta):
        tablename = "api_rxnorm_ingredient"
    rxcui: str = ormar.String(max_length=1000, primary_key=True)
    name: str = ormar.String(max_length=5000)
    tty: str = ormar.String(max_length=1000)
    active: str = ormar.String(max_length=1000)
    prescribable: str = ormar.String(max_length=1000)


class clinical_products(ormar.Model):
    class Meta(BaseMeta):
        tablename = "api_rxnorm_clinical_product"
    rxcui: str = ormar.String(max_length=20, primary_key=True)
    name: str = ormar.String(max_length=5000)
    tty: str = ormar.String(max_length=5)
    active: str = ormar.String(max_length=10)
    prescribable: str = ormar.String(max_length=10)


class dailymed_rxnorms(ormar.Model):
    class Meta(BaseMeta):
        tablename = "api_dailymed_rxnorm"
    id: str = ormar.String(max_length=1000, primary_key=True)
    setid: str = ormar.String(max_length=100)
    rxcui: str = ormar.String(max_length=20)
    rxstr: str = ormar.String(max_length=5000)
    rxtty: str = ormar.String(max_length=5)


engine = sqlalchemy.create_engine(settings.db_url)
metadata.create_all(engine)
 