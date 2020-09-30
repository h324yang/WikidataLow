from bson.json_util import dumps, loads
from sklearn.model_selection import train_test_split


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
    

def train_dev_test_split(ent_ids, train_test_ratio=0.7, train_dev_ratio=0.9):
    train, test, _, _ = train_test_split(
        ent_ids, 
        [1]*len(ent_ids), 
        train_size=train_test_ratio, 
        test_size=(1. - train_test_ratio)
    )
    
    train, dev, _, _ = train_test_split(
        train, 
        [1]*len(train), 
        train_size=train_dev_ratio, 
        test_size=(1 - train_dev_ratio)
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