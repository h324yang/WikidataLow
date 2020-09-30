import pymongo
import json
from bson.json_util import dumps, loads


DB_NAME = "wikidata-20200921"
IP = "mongodb://localhost:27017/"

def main():
    client = pymongo.MongoClient(IP)
    db = client[DB_NAME]
    col = db['entity']
    col.create_index("id")


if __name__ == "__main__":
    main()