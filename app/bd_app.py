import csv
from pprint import pprint

from pymongo import MongoClient

clint = MongoClient()

db = clint["analitics"]

with open("fuck.csv") as f:
    reader = csv.DictReader(f, delimiter=";")
    # pprint(list(reader))
    db["c"].insert_many(list(reader))

pprint(list(db["c"].find()))
