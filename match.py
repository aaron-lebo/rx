from datetime import datetime
import sqlite3
import os

import click
import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import DocBin

nlp = spacy.load('en_core_web_lg')

@click.command()
@click.argument('tag')
@click.argument('files', nargs=-1)
def match(tag, files):
    terms, combos = set(), []
    with open('terms.txt') as f:
        for x in f.readlines():
            x = {y.strip() for y in x.split(',')}
            terms.update(x)
            if len(x) > 1: 
                combos.append(x)

    m = matcher = PhraseMatcher(nlp.vocab, attr='LOWER')
    m.add(tag, [nlp.make_doc(x) for x in terms])

    nfiles = len(files)
    for i, f in enumerate(files):
        bin = DocBin().from_disk(f)
        n = len(bin)
        bin1 = DocBin(attrs=[])
        t = start = datetime.now()
        for j, d in enumerate(bin.get_docs(nlp.vocab)):
            ms = matcher(d)
            if ms:
                d.user_data['matches'] = ms
                ms = {d[bgn:end].text.lower() for _, bgn, end in ms}
                for x in terms:
                    if ms.issubset(x):
                        d.user_user['combo'] = ms
                        break

                bin1.add(d)

            end = n == j + 1
            t1 = datetime.now()
            if end or (t1-t).total_seconds() > 1:
                end = '\n' if end else '\r'
                print(f'{i:3,} {nfiles}  {f}  {n:12,}  {(j+1)/n*100:6.2f}%  {t1-start}', end=end)
                t = t1

        f = f'match/{os.path.split(f)[1]}'
        if os.path.isfile(f):
            raise FileExistsError

        os.makedirs('match', exist_ok=1)
        bin1.to_disk(f)

if __name__ == '__main__':
    match()
