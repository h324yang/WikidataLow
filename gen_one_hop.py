import pymongo
import json
from bson.json_util import dumps, loads
import pickle
from util.graph import collect_one_hop
from util import load_entities_from_jsonl


LANGS = open("./meta/low-langs.txt").read().split() + ["en"]
INTERSECTION = "meta/intersection.jsonl"
OUTPUT = "./meta/one_hop_{elem}.jsonl"
DB_NAME = "wikidata-20200921"
IP = "mongodb://localhost:27017/"


def main():
    client = pymongo.MongoClient(IP)
    db = client[DB_NAME]
    col = db['entity']
    intersection_ent = load_entities_from_jsonl(INTERSECTION)
    triples, attributes = collect_one_hop(intersection_ent)
    
    # find new entities
    all_ent = set()
    for s, r, o in triples:
        all_ent.add(s)
        all_ent.add(o)
        
    new_ent = all_ent - set(intersection_ent.keys())
    
    # write new entities
    with open(OUTPUT.format(elem="ent"), "w") as f:
        for idx, ent in enumerate(new_ent):
            cur = col.find_one({"id": ent})
            f.write(dumps(cur) + "\n")
            if ( idx + 1 ) % 1000 == 0:
                print( (idx + 1) )
                
    # find all properties
    all_prop = set()
    for s, prop, o in triples:
        all_prop.add(prop)

    for ent, attrs in attributes.items():
        for prop in attrs:
            all_prop.add(prop)
            
    # write all properties
    with open(OUTPUT.format(elem="prop"), "w") as f:
        for idx, prop in enumerate(all_prop):
            cur = col.find_one({"id": prop})
            f.write(dumps(cur) + "\n")
            if ( idx + 1 ) % 1000 == 0:
                print( (idx + 1) )
            

if __name__ == "__main__":
    main()