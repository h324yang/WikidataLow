import pymongo
import json
from bson.json_util import dumps, loads


LANGS = open("./meta/low-langs.txt").read().split() + ["en"]
OUTPUT = "./meta/intersection.jsonl"
DB_NAME = "wikidata-20200921"
IP = "mongodb://localhost:27017/"


def main():
    client = pymongo.MongoClient(IP)
    db = client[DB_NAME]
    col = db['entity']
    query = {"$and": []}
    for lang in LANGS:
        query["$and"].append({f"sitelinks.{lang}wiki": {"$exists": 1}})
    cur = col.find(query)
    
    with open(OUTPUT, "w") as f:
        for i, doc in enumerate(cur):
            print(f"Writing doc #{i}...")
            f.write(dumps(doc)+"\n")


if __name__ == "__main__":
    main()