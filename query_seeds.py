import pymongo
import json
from bson.json_util import dumps, loads
from util import Timer
import random


LANGS = open("./meta/low-langs.txt").read().split()
DB_NAME = "wikidata-20200921"
IP = "mongodb://localhost:27017/"
DATA_OUTPUT = "./meta/seeds.data.union.jsonl"
ID_OUTPUT = "./meta/seeds.id.{lang}.txt"
SIZE = 15000


def main():
    client = pymongo.MongoClient(IP)
    db = client[DB_NAME]
    col = db['lang']
    timer = Timer()
    
    samples = {}
    for lang in LANGS:
        lang_samples = [s for s in col.find({f"{lang}wiki": 1})]
        lang_samples = random.sample(lang_samples, SIZE)
        if not lang in samples:
            samples[lang] = set()

        for ent in lang_samples:
            samples[lang].add(ent['id'])
    
    for lang, ents in samples.items():
        with open(ID_OUTPUT.format(lang=lang), "w") as f:
            for ent in ents:
                ent_langs = col.find_one({"id": ent})
                enwiki_exists = ent_langs["enwiki"]
                f.write(f"{ent}\t{enwiki_exists}\n")
    
    union = set()
    for ents in samples.values():
        if not union:
            union.update(ents)
        else:
            union = union.union(ents)
    
    with open(DATA_OUTPUT, "w") as f:
        timer.start()
        union_size = len(union)
        col = db['entity']
        for i, ent in enumerate(union):
            doc = col.find_one({"id": ent})
            f.write(dumps(doc)+"\n")               
            if (i + 1) % 10000 == 0:
                time_spent = f"[{timer.diff():.2f} sec]"
                print(f"{i + 1}/{union_size} written..." + time_spent)


if __name__ == "__main__":
    main()