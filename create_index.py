import pymongo
import json
from bson.json_util import dumps, loads


DB_NAME = "wikidata-20200921"
IP = "mongodb://localhost:27017/"
LANGS = open("./meta/low-langs.txt").read().split() + ["en"]

def main():
    client = pymongo.MongoClient(IP)
    db = client[DB_NAME]
    col = db['entity']
    col.create_index("id")
    
    col = db['lang']
    col.create_index("id")
    for lang in LANGS:
        col.create_index(f"{lang}wiki")
        

if __name__ == "__main__":
    main()