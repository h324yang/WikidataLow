from bson.json_util import dumps, loads
from sklearn.model_selection import train_test_split
import os
import time

def load_seed_ids_from_txt(fpath):
    seeds = {}
    with open(fpath) as f:
        for line in f:
            if line:
                eid, en_exists = line.strip().split()
                seeds[eid] = int(en_exists)
    return seeds


def collect_all_seeds(seed_path_map):
    all_seeds = {'en':dict()}
    for lang, fpath in seed_path_map.items():
        lang_seeds = load_seed_ids_from_txt(fpath)
        all_seeds[lang] = lang_seeds
        # EN seeds are the union of ILLs to EN across all langs
        for eid, en_exists in lang_seeds.items():
            if en_exists:
                all_seeds['en'][eid] = 1
    return all_seeds


def load_entities_from_jsonl(fpath):
    ent = {}
    with open(fpath) as f:
        for line in f:
            doc = loads(line.strip())
            if doc:
                ent[doc['id']] = doc
    return ent


def load_attributes(fpath):
    attributes = {}
    with open(fpath) as f:
        for line in f:
            doc = line.strip().split("\t")
            if doc:
                attributes[doc[0]] = doc[1:]
    return attributes


class Timer:
    def __init__(self):
        pass
    
    def start(self):
        self.start = time.time()
        self.last = self.start
        
    def diff(self):
        cur = time.time()
        delta = cur - self.last
        self.last = cur
        return delta
    
    def elapsed(self):
        cur = time.time()
        delta = cur - self.start
        return delta
    

def train_dev_test_split(ent_ids, train_test_ratio=0.7, train_dev_ratio=0.9):
    train, test, _, _ = train_test_split(
        ent_ids, 
        [1]*len(ent_ids), 
        train_size=train_test_ratio, 
        test_size=round(1. - train_test_ratio, 3)
    )
    
    train, dev, _, _ = train_test_split(
        train, 
        [1]*len(train), 
        train_size=train_dev_ratio, 
        test_size=round(1 - train_dev_ratio, 3)
    )
    
    return train, dev, test


def load_triples(fpath):
    triples = []
    with open(fpath) as f:
        for line in f:
            if line:
                triple = tuple(line.strip().split("\t"))
                triples.append(triple)
    return triples


def count_lines_from_file(fpath):
    cnt = 0
    with open(os.path.join(fpath)) as f:
        for line in f: cnt += 1
    return cnt


def count_rels_from_triples(fpath):
    rels = set()
    for s, p, o in load_triples(fpath):
        rels.add(p)
    return len(rels)


def count_attributes(fpath):
    attrs = set()
    for ent, ent_attrs in load_attributes(fpath).items():
        attrs.update(set(ent_attrs))
    return len(attrs)


def count_attribute_triples(fpath):
    cnt = 0
    for ent, ent_attrs in load_attributes(fpath).items():
        cnt += len(ent_attrs)
    return cnt


class Stats:
    def __init__(self, data_dir):
        self.data_dir = data_dir
              
    def analyze(self):
        funcs = [attr for attr in dir(self) if attr.startswith("get")]
        record_1 = {"data_dir": self.data_dir, "lang": 1}
        record_2 = {"data_dir": self.data_dir, "lang": 2}
        for func in funcs:
            this_func = getattr(self, func)
            cnt_1, cnt_2 = this_func()
            record_1[func[4:]] = cnt_1
            record_2[func[4:]] = cnt_2
        return record_1, record_2
                      
    def get_num_entities(self):
        cnt_1 = count_lines_from_file(os.path.join(self.data_dir, "ent_ids_1"))
        cnt_2 = count_lines_from_file(os.path.join(self.data_dir, "ent_ids_2"))
        return cnt_1, cnt_2
    
    def get_num_relations(self):
        cnt_1 = count_rels_from_triples(os.path.join(self.data_dir, "triples_1"))
        cnt_2 = count_rels_from_triples(os.path.join(self.data_dir, "triples_2"))
        return cnt_1, cnt_2

    def get_num_attributes(self):
        cnt_1 = count_attributes(os.path.join(self.data_dir, "training_attrs_1"))
        cnt_2 = count_attributes(os.path.join(self.data_dir, "training_attrs_2"))
        return cnt_1, cnt_2

    def get_num_relation_triples(self):
        cnt_1 = count_lines_from_file(os.path.join(self.data_dir, "triples_1"))
        cnt_2 = count_lines_from_file(os.path.join(self.data_dir, "triples_2"))
        return cnt_1, cnt_2    
              
    def get_num_attribute_triples(self):
        fpath_1 = os.path.join(self.data_dir, "training_attrs_1")
        fpath_2 = os.path.join(self.data_dir, "training_attrs_2")
        cnt_1 = count_attribute_triples(fpath_1)
        cnt_2 = count_attribute_triples(fpath_2)
        return cnt_1, cnt_2
              
    def get_num_descriptions(self):
        cnt_1 = count_lines_from_file(os.path.join(self.data_dir, "comment_1"))
        cnt_2 = count_lines_from_file(os.path.join(self.data_dir, "comment_2"))
        return cnt_1, cnt_2  