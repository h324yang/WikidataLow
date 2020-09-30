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
    with bz2.open(DUMP_PATH) as f:
        timer.start()
        for ln, line in enumerate(f):
            try:
                line = line.decode().strip()
                entity = json.loads(line[:-1])
                res = col.insert_one(entity)

            except Exception as e:
                print(e)

            if (ln + 1) % 1000 == 0:
                print(f"{ln + 1} entities inserted...[{timer.diff():.2f} sec]")
    

if __name__ == "__main__":
    main()