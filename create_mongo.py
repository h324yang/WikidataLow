import pymongo
import bz2
import json
from tqdm import tqdm
import time
from util import Timer


DUMP_PATH = "dumps/wikidata-20200921-all.json.bz2"
LANGS = open("./meta/low-langs.txt").read().split() + ["en"]
OUTPUT = "./meta/intersection.txt"
DB_NAME = "wikidata-20200921"
IP = "mongodb://localhost:27017/"
    
def main():
    client = pymongo.MongoClient(IP)
    db = client[DB_NAME]
    col = db['entity']
    timer = Timer()
    timer.start()
    
    with bz2.open(DUMP_PATH) as f:
        for ln, line in enumerate(f):
            try:
                line = line.decode().strip()
                entity = json.loads(line[:-1])
                res = col.insert_one(entity)

            except Exception as e:
                print(e)

            if (ln + 1) % 1000 == 0:
                print(f"{ln + 1} entities inserted...[{timer.diff():.2f} sec]")
                
    new_col = db['lang']
    docs = col.find({"id": {"$regex":"^Q"}})
    for idx, doc in enumerate(docs):
        cleaned_doc = {'id': doc['id']} 
        wiki_langs = set(doc['sitelinks'].keys())
        for lang in LANGS + ['en']:
            if f"{lang}wiki" in wiki_langs:
                cleaned_doc[f"{lang}wiki"] = 1
            else:
                cleaned_doc[f"{lang}wiki"] = 0
            
        new_col.insert_one(cleaned_doc)
        if (idx + 1) % 1000 == 0:
            time_spent = f"[{timer.diff():.2f} sec]"
            print(f"{idx + 1} entities' languages indexed..." + time_spent)
    

if __name__ == "__main__":
    main()