from util.graph import collect_one_hop, LangChecker
from util import load_entities_from_jsonl
import os


LANGS = open("./meta/low-langs.txt").read().split() + ["en"]
INTERSECTION = "meta/intersection.jsonl"
ONE_HOP = "./meta/one_hop_{elem}.jsonl"
OUTPUT_DIR = "./kg"


def main():
    intersection_ent = load_entities_from_jsonl(INTERSECTION)
    new_ent = load_entities_from_jsonl(ONE_HOP.format(elem="ent"))
    new_ent.update(intersection_ent)
    ents = new_ent
    props = load_entities_from_jsonl(ONE_HOP.format(elem="prop"))
    triples, attributes = collect_one_hop(intersection_ent)
    lang_checker = LangChecker(ents, props)
    
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)
    
    for lang in LANGS:
        lang_dir = os.path.join(OUTPUT_DIR, lang)
        if not os.path.exists(lang_dir):
            os.mkdir(lang_dir)
        
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
        
        ent_ids = set()
        for s, r, o in lang_triples:
            ent_ids.add(s)
            ent_ids.add(o)
            
        ent_ids = sorted(ent_ids, key=lambda x: int(x[1:]))
        
        with open(os.path.join(lang_dir, "triples"), "w") as f:
            for triple in lang_triples:
                f.write("\t".join(triple)+"\n")
                
        with open(os.path.join(lang_dir, "attributes"), "w") as f:
            for ent_id in ent_ids:
                if ent_id in lang_attributes:
                    attrs = set(lang_attributes[ent_id])
                    attrs = sorted(attrs, key=lambda x: int(x[1:]))
                    f.write("\t".join([ent_id] + attrs)+"\n")
                    
        

if __name__ == "__main__":
    main()