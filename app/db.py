import databases
import ormar
import sqlalchemy

from .config import settings

database = databases.Database(settings.db_url)
metadata = sqlalchemy.MetaData()


class BaseMeta(ormar.ModelMeta):
    metadata = metadata
    database = database


class ingredients(ormar.Model):
    class Meta(BaseMeta):
        tablename = "rxnorm_ingredient"
    ingredient_rxcui: str = ormar.String(max_length=1000, primary_key=True)
    ingredient_name: str = ormar.String(max_length=5000)
    ingredient_tty: str = ormar.String(max_length=1000)
    active: str = ormar.String(max_length=1000)
    prescribable: str = ormar.String(max_length=1000)


#class prices(ormar.Model):
#    class Meta(BaseMeta):

engine = sqlalchemy.create_engine(settings.db_url)
metadata.create_all(engine)
