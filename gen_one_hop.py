import pymongo
import json
from bson.json_util import dumps, loads
import pickle
from util.graph import collect_one_hop
from util import load_entities_from_jsonl, collect_all_seeds


SEED_DATA = "./meta/seeds.data.union.jsonl"
SEED_ID = "./meta/seeds.id.{lang}.txt"
OUTPUT = "./meta/one_hop.{elem}.union.jsonl"
DB_NAME = "wikidata-20200921"
IP = "mongodb://localhost:27017/"
LANGS = open("./meta/low-langs.txt").read().split()


def main():
    client = pymongo.MongoClient(IP)
    db = client[DB_NAME]
    col = db['entity']
    seed_ents = load_entities_from_jsonl(SEED_DATA)
    
    # collect all seeds
    seed_path_map = {lang: SEED_ID.format(lang=lang) for lang in LANGS}
    seeds = collect_all_seeds(seed_path_map)
    
    # print stats
    for lang, lang_seeds in seeds.items():
        print(f"#{lang}: {len(lang_seeds)}")
    
    new_ents = set()
    all_props = set()
    for lang in LANGS + ['en']:
        cur_seed_ents = {eid: seed_ents[eid] for eid in seeds[lang].keys()}
        triples, attributes = collect_one_hop(cur_seed_ents)
    
        # find new entities
        for s, r, o in triples:
            if not s in seed_ents: new_ents.add(s)
            if not o in seed_ents: new_ents.add(o)

        # find all properties
        for s, prop, o in triples:
            all_props.add(prop)

        for ent, attrs in attributes.items():
            for prop in attrs:
                all_props.add(prop)
                
    # write all entities
    total = len(new_ents)
    with open(OUTPUT.format(elem="ent"), "w") as f:
        for idx, ent in enumerate(new_ents):
            cur = col.find_one({"id": ent})
            f.write(dumps(cur) + "\n")
            if (idx + 1) % 1000 == 0:
                print(f"{idx + 1}/{total}")

    # write all properties
    total = len(all_props)
    with open(OUTPUT.format(elem="prop"), "w") as f:
        for idx, prop in enumerate(all_props):
            cur = col.find_one({"id": prop})
            f.write(dumps(cur) + "\n")
            if (idx + 1) % 1000 == 0:
                print(f"{idx + 1}/{total}")
            

if __name__ == "__main__":
    main()