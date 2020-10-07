import os
from util import (
    load_refs_from_txt,
    load_descriptions
)
import random
import json

LANGS = open("meta/low-langs.txt").read().strip().split()
OUTPUT_DIR = "examples"
NUM_NEG_SAMPLES = 1
RANDOM_SEED = 1234

random.seed(RANDOM_SEED)

def main():
    for lang in LANGS:
        print(lang)
        desc_1 = load_descriptions(f"data/{lang}_en/comment_1")
        desc_2 = load_descriptions(f"data/{lang}_en/comment_2")
        
        for split in ("train", "dev", "test"):
            examples = []
            cur_refs = load_refs_from_txt(f"data/{lang}_en/{split}")
            tails = set([tail for head, tail in cur_refs])
            for center, pos in cur_refs:
                text_center = desc_1[center]
                if pos == "-1":
                    text_pos = "NIL"
                else:
                    text_pos = desc_2[pos]
                
                for _ in range(NUM_NEG_SAMPLES):
                    while True:
                        neg = random.sample(tails, 1)[0]
                        if neg != pos and neg != "-1": 
                            break
                    
                    text_neg = desc_2[neg]
                    guid = f"{center}-{pos}-{neg}"
                    example = {
                        "guid": guid, 
                        "text_center": text_center, 
                        "text_pos": text_pos, 
                        "text_neg": text_neg
                    }
                    
                    examples.append(example)
                    
        random.shuffle(examples)
        opath = f"{lang}_en.triples.text.jsonl"
        with open(os.path.join(OUTPUT_DIR, opath), "w") as f:
            last = len(examples)
            for i, example in enumerate(examples):
                if (i + 1) == last:
                    f.write(json.dumps(example))
                else:
                    f.write(json.dumps(example)+"\n")
                    

if __name__ == "__main__":
    main()