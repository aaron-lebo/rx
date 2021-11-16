import os

import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import DocBin
from tqdm.auto import tqdm
import typer
from typing import List

nlp = spacy.load('en_core_web_lg')

def main(tag: str, terms_file: str, files: List[str]):
    terms, combos, raw = set(), [], []
    with open(terms_file) as f:
        for x in f:
            ands = []
            for and_ in x.split(';'):
                ands.append({x.strip() for x in and_.split(',')})
                terms.update(ands[-1])

            combos.append(ands)
            raw.append(x.strip())

    m = matcher = PhraseMatcher(nlp.vocab, attr='LOWER')
    m.add(tag, [nlp.make_doc(x) for x in terms])

    os.makedirs('out/match', exist_ok=True)
    for i, f in enumerate(files):
        bin = DocBin().from_disk(f)
        bar = tqdm(bin.get_docs(nlp.vocab), total=len(bin))
        bar.set_description(f'{i}/{len(files)} {f}')
        results = [] 
        for d in bar:
            ms = matcher(d)
            if not ms: 
                continue

            ms = {d[bg:nd].text.lower() for _, bg, nd in ms}
            res = [d.user_data['id'], ','.join(ms), '']
            for j, x in enumerate(combos):
                ok = True
                for y in x:
                    if not ms & y: 
                        ok = False
                        break

                if ok:
                    res[2] = raw[j]
                    break

            results.append(res)

    f = f'out/match/{os.path.split(f)[1].replace(".spacy", ".csv")}'
    if os.path.isfile(f):
        raise FileExistsError

    df = pd.DataFrame(results, columns=['id', 'matches', 'combos'])
    df = df.set_index('id')
    df.to_csv(f)

if __name__ == '__main__':
    typer.run(main)
