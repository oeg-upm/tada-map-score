from peewee import SqliteDatabase, Model, CharField, IntegerField
from peewee import *

# DATABASE = ':memory:'
DATABASE = 'data.db'

database = SqliteDatabase(DATABASE)

STATUS_COMPLETE = "complete"
STATUS_NEW = "new"
STATUS_INPROGRESS = "inprogress"

STATUSES = [STATUS_NEW, STATUS_INPROGRESS, STATUS_COMPLETE]

# def get_database(name=DATABASE):
#     database = SqliteDatabase(name)
#     return database


class BaseModel(Model):
    class Meta:
        database = database


class Bite(BaseModel):
    table = CharField()  # table name or id
    column = IntegerField()  # column order (position)
    slice = IntegerField()  # slice order (position)
    addr = CharField()  # the url:ip of the server that the processed data need to be sent to
    fname = CharField()  # the name of the uploaded file
    total = IntegerField()  # total number of slices, to be sent to the combine
    status = CharField(default=STATUS_NEW, choices=STATUSES)

    def json(self):
        return{
            "id": self.id,
            "table": self.table,
            "slice": self.slice,
            "addr": self.addr,
            "fname": self.fname,
            "total": self.total,
            "status": self.status,
        }


def create_tables():
    with database:
        database.create_tables([Bite, ])