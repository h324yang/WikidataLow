def get_ent_and_prop_ids(triples, seed_ents=None):
    ent_ids = set()
    prop_ids = set()
    for s, p, o in triples:
        ent_ids.add(s)
        ent_ids.add(o)
        prop_ids.add(p)
        
    if seed_ents:
        ent_ids = ent_ids.union(set(seed_ents))
        
    ent_ids = sorted(ent_ids, key=lambda x: int(x[1]))
    ent_ids = {ent: idx for idx, ent in enumerate(ent_ids)}
    prop_ids = sorted(prop_ids, key=lambda x: int(x[1]))
    prop_ids = {prop: idx for idx, prop in enumerate(prop_ids)}
    return ent_ids, prop_ids


def collect_one_hop(seed_entities):
    """ get one-hop lang-independent neighborghs
    """
    triples = []
    attributes = {}
    for ent, data in seed_entities.items():
        for prop, vals in data['claims'].items():
            for val in vals:
                rank = val['rank']
                snaktype = val['mainsnak']['snaktype']
                if rank != "deprecated" and snaktype == 'value':
                    datavalue = val['mainsnak']['datavalue']
                    if datavalue['type'] == 'wikibase-entityid':
                        obj = datavalue['value']['id']
                        if obj.startswith('Q'):
                            triples.append((ent, prop, obj))

                    else:
                        if not ent in attributes:
                            attributes[ent] = []
                        attributes[ent].append(prop)
                        
    return triples, attributes


class LangChecker:
    def __init__(self, ents, props):
        self.ents = ents
        self.props = props
        
    def prop_lang_exists(self, langs, prop):
        try:
            prop_exists = False
            for lang in langs:
                if lang in self.props[prop]["labels"]:
                    prop_exists = True
            return prop_exists
        except:
            print(f"Error when fetching {prop} ({lang}).")
    
    def ent_lang_exists(self, lang, ent):
        try:
            return f"{lang}wiki" in self.ents[ent]["sitelinks"]
        except:
            print(f"Error when fetching {ent} ({lang}).")
            
    def extract_triples_from_lang(self, ent_lang, prop_langs, triples):
        lang_triples = []
        for sub, prop, obj in triples:
            sub_exists = self.ent_lang_exists(ent_lang, sub)
            obj_exists = self.ent_lang_exists(ent_lang, obj)
            prop_exists = self.prop_lang_exists(prop_langs, prop)
                
            if sub_exists and prop_exists and obj_exists:
                lang_triples.append((sub, prop, obj))
            else:
                continue
        return lang_triples
    
    def extract_attributes_from_lang(self, ent_lang, prop_langs, attributes):
        lang_attributes = {}
        for ent, attrs in attributes.items():
            ent_exists = self.ent_lang_exists(ent_lang, ent)
            if ent_exists:
                if not ent in lang_attributes:
                    lang_attributes[ent] = []
                
                for prop in attrs:
                    prop_exists = self.prop_lang_exists(prop_langs, prop)
                    if prop_exists:
                        lang_attributes[ent].append(prop)
        return lang_attributes
    
    def extract_desc_from_lang(self, ent_id, lang):
        desc = self.ents[ent_id]['descriptions']
        if lang in desc:
            return desc[lang]['value']
        else:
            return None