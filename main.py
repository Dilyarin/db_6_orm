import sqlalchemy
import sqlalchemy as sq
import psycopg2
from sqlalchemy import engine
from pprint import pprint
import pandas as pd
import json

from sqlalchemy.orm import sessionmaker, declarative_base, relationship

from model import create_tables, Shop, Publisher, Book, Stock, Sale

DSN = 'postgresql://postgres:postgres@localhost:5432/HW_6'
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

res = input('id = ')
q = session.query(Publisher).filter(Publisher.id == res)
for i in q.all():
    print(f'Издатель: {i.name}')

session.close()

