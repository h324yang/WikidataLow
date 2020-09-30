import os
from util import (
    load_entities_from_jsonl, 
    train_dev_test_split, 
    load_triples
)

from util.graph import get_ent_and_prop_ids, LangChecker


LANGS = open("./meta/low-langs.txt").read().split()
INTERSECTION = "meta/intersection.jsonl"
ONE_HOP = "./meta/one_hop_{elem}.jsonl"
OUTPUT_DIR = "./data"
KG_DIR = "./kg"


def main():
    intersection_ent = load_entities_from_jsonl(INTERSECTION)
    new_ent = load_entities_from_jsonl(ONE_HOP.format(elem="ent"))
    new_ent.update(intersection_ent)
    ents = new_ent
    lang_checker = LangChecker(ents, None)
    
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)
    
    ref = list(intersection_ent.keys())
    train, dev, test = train_dev_test_split(ref)
    en_triples = load_triples(os.path.join(KG_DIR, "en/triples"))
    en_ent_ids, en_prop_ids = get_ent_and_prop_ids(en_triples, ref)
    
    for lang in LANGS:
        lang_dir = os.path.join(OUTPUT_DIR, f"{lang}_en")
        if not os.path.exists(lang_dir):
            os.mkdir(lang_dir)
            
        lang_triples = load_triples(os.path.join(KG_DIR, f"{lang}/triples"))
        lang_ent_ids, lang_prop_ids = get_ent_and_prop_ids(lang_triples, ref)
        
        # id-entity mapping
        with open(os.path.join(lang_dir, "ent_ids_1"), "w") as f:
            ids = [f"{idx}\t{ent}" for ent, idx in lang_ent_ids.items()]
            f.write('\n'.join(ids))
        
        offset = len(lang_ent_ids)
        with open(os.path.join(lang_dir, "ent_ids_2"), "w") as f:
            ids = [f"{idx+offset}\t{ent}" for ent, idx in en_ent_ids.items()]
            f.write('\n'.join(ids))
        
        # descriptions
        with open(os.path.join(lang_dir, "comment_1"), "w") as f:
            desc = []
            for ent, idx in lang_ent_ids.items():
                if lang == "zh_yue":
                    cur_desc = None
                    for yue in ["zh-yue", "yue", "zh-hk"]:
                        cur_desc = lang_checker.extract_desc_from_lang(ent, yue)
                        if cur_desc: break
                            
                else:
                    cur_desc = lang_checker.extract_desc_from_lang(ent, lang)
                    
                if cur_desc:
                    desc.append(f"{idx}\t{cur_desc}")

            f.write('\n'.join(desc))
            
        with open(os.path.join(lang_dir, "comment_2"), "w") as f:
            desc = []
            for ent, idx in en_ent_ids.items():
                cur_desc = lang_checker.extract_desc_from_lang(ent, "en")
                if cur_desc:
                    desc.append(f"{idx}\t{cur_desc}")
            f.write('\n'.join(desc))
        
        # indexed triples
        prop_ids = set(lang_prop_ids.keys()).union(set(en_prop_ids.keys()))
        prop_ids = sorted(prop_ids, key=lambda x: int(x[1]))
        prop_ids = {prop: idx for idx, prop in enumerate(prop_ids)}
        
        with open(os.path.join(lang_dir, "triples_1"), "w") as f:
            triples = []
            for s, p, o in lang_triples:
                triple = (
                    str(lang_ent_ids[s]), 
                    str(prop_ids[p]), 
                    str(lang_ent_ids[o])
                )
                triples.append("\t".join(triple))
            f.write("\n".join(triples))
            
        with open(os.path.join(lang_dir, "triples_2"), "w") as f:
            triples = []
            for s, p, o in en_triples:
                triple = (
                    str(en_ent_ids[s] + offset), 
                    str(prop_ids[p]), 
                    str(en_ent_ids[o] + offset)
                )
                
                triples.append("\t".join(triple))
            f.write("\n".join(triples))
            
        # attributes
        os.system(f"cp {KG_DIR}/{lang}/attributes {lang_dir}/training_attrs_1")  
        os.system(f"cp {KG_DIR}/en/attributes {lang_dir}/training_attrs_2")
        
        # ILL splits        
        settings = [
            ("ref_ent_ids", ref), 
            ("train", train),
            ("dev", dev), 
            ("test", test)
        ]
        
        for fpath, seeds in settings:
            with open(os.path.join(lang_dir, fpath), "w") as f:
                ILLs = []
                for ent in sorted(seeds, key=lambda x: int(x[1:])):
                    ILL = (str(lang_ent_ids[ent]), str(en_ent_ids[ent]+offset))
                    ILLs.append("\t".join(ILL))
                f.write("\n".join(ILLs))
    

if __name__ == "__main__":
    main()