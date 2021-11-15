from collections import defaultdict
import json

import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import DocBin
from tqdm.auto import tqdm
import typer
from typing import List

nlp = spacy.load('en_core_web_lg')

def main(tag: str, terms_file: str, files: List[str]):
    terms, combos = set(), []
    with open(terms_file) as f:
        for x in f:
            ands = []
            for and_ in x.split(';'):
                ands.append({x.strip() for x in and_.split(',')})
                terms.update(ands[-1])

            combos.append(ands)

    m = matcher = PhraseMatcher(nlp.vocab, attr='LOWER')
    m.add(tag, [nlp.make_doc(x) for x in terms])

    for i, f in enumerate(files):
        bin = DocBin().from_disk(f)
        bar = tqdm(bin.get_docs(nlp.vocab), total=len(bin))
        bar.set_description(f'{i}/{len(files)} {f}')
        matches, combos_ = defaultdict(set), defaultdict(set)
        for d in bar:
            ms = matcher(d)
            if not ms: 
                continue

            ms = {d[bg:nd].text.lower() for _, bg, nd in ms}
            ms_ = ','.join(ms)
            id = d.user_data['id']
            matches[ms_].update(id)
            for x in combos:
                ok = True
                for y in x:
                    if not ms & y: 
                        ok = False
                        break

                if ok:
                    combos_[ms_].update(id)
                    break

        with open(f.replace('.spacy', '.json'), 'wt') as f:
            json.dump(dict(
                matches = {k: list(v) for k, v in matches.items()}, 
                combos = {k: list(v) for k, v in combos_.items()}
            ), f)

if __name__ == '__main__':
    typer.run(main)
