import databases
import ormar
import sqlalchemy

from .config import settings

database = databases.Database(settings.db_url)
metadata = sqlalchemy.MetaData()


class BaseMeta(ormar.ModelMeta):
    metadata = metadata
    database = database


class ndc(ormar.Model):
    class Meta(BaseMeta):
        tablename = "ndc"

#class prices(ormar.Model):
#    class Meta(BaseMeta):

engine = sqlalchemy.create_engine(settings.db_url)
metadata.create_all(engine)
