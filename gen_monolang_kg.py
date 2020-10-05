from util.graph import collect_one_hop, LangChecker
from util import load_entities_from_jsonl, collect_all_seeds
from collections import ChainMap
import os


SEED_DATA = "./meta/seeds.data.union.jsonl"
SEED_ID = "./meta/seeds.id.{lang}.txt"
ONE_HOP = "./meta/one_hop.{elem}.union.jsonl"
LANGS = open("./meta/low-langs.txt").read().split()
OUTPUT_DIR = "./kg"


def main():
    seed_ents = load_entities_from_jsonl(SEED_DATA)
    new_ents = load_entities_from_jsonl(ONE_HOP.format(elem="ent"))
    ents = ChainMap(seed_ents, new_ents)
    props = load_entities_from_jsonl(ONE_HOP.format(elem="prop"))
    lang_checker = LangChecker(ents, props)
    
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)
    
    # collect all seeds
    seed_path_map = {lang: SEED_ID.format(lang=lang) for lang in LANGS}
    seeds = collect_all_seeds(seed_path_map)
    
    for lang in LANGS + ["en"]:
        lang_dir = os.path.join(OUTPUT_DIR, lang)
        if not os.path.exists(lang_dir):
            os.mkdir(lang_dir)
            
        lang_seed_ents = {}
        for eid in seeds[lang].keys():
            lang_seed_ents[eid] = ents[eid]
            
        triples, attributes = collect_one_hop(lang_seed_ents)

        ent_lang = lang
        if lang == "zh_yue":
            prop_langs = ["yue", "zh-yue"]
        else:
            prop_langs = [lang]

        lang_triples = lang_checker.extract_triples_from_lang(
            ent_lang, 
            prop_langs, 
            triples
        )
        
        lang_attributes = lang_checker.extract_attributes_from_lang(
            ent_lang, 
            prop_langs, 
            attributes
        )
        
        with open(os.path.join(lang_dir, "triples"), "w") as f:
            for triple in lang_triples:
                f.write("\t".join(triple)+"\n")
                
        with open(os.path.join(lang_dir, "attributes"), "w") as f:
            ent_ids = sorted(lang_attributes.keys(), key=lambda x: int(x[1:]))
            for ent_id in ent_ids:
                attrs = set(lang_attributes[ent_id])
                attrs = sorted(attrs, key=lambda x: int(x[1:]))
                f.write("\t".join([ent_id] + attrs)+"\n")
        

if __name__ == "__main__":
    main()