import csv
import glob
import os

import pandas as pd
import spacy
from spacy import matcher
from spacy.tokens import DocBin
from tqdm.auto import tqdm
import typer

app = typer.Typer()
nlp = spacy.load('en_core_web_lg')

def data(dat, cols):
    df = pd.DataFrame(dat, columns=cols.split())
    df = df.set_index('id')
    return df

def clean(txt: str):
    prs = {'(': ')', '[': ']', '{': '}'}
    stk = []
    for i, x in enumerate(txt):
        if x in prs:
            stk.append((i, x))
        elif stk and x == prs[stk[-1][1]]:
            stk.pop()
        elif stk and x in prs or x in prs.values():
            txt = txt[:i]
    return txt[:stk[0][0]] if stk else txt

def save(df, f):
    if os.path.isfile(f):
        raise FileExistsError
    df.to_csv(f)

stats = pd.read_csv('stats.csv')
stats.file = stats.file.str.lower()
stats = dict(stats.values)

@app.command()
def urls(dir: str):
    dir = [x for x in dir.split('/') if x][-1]
    rs_ = dir.startswith('rs_')
    m = match = matcher.Matcher(nlp.vocab)
    m.add('url',  [[{'LIKE_URL': True}], [{'LOWER': {'REGEX': r'\)\[http(.*)'}}]])
    os.makedirs(f'out/{dir}/match/urls', exist_ok=True)

    n, fs = 0, glob.glob(f'{dir}/*.spacy')
    for i, f in tqdm(enumerate(fs), total=len(fs)):
        dat, bin = [], DocBin().from_disk(f)
        n += len(bin)
        for d in bin.get_docs(nlp.vocab):
            ud = d.user_data
            if rs_:
                pre = ud['url'].split('://')
                dat.append([ud['id'], pre[0][-1], pre[-1]])

            for _, y, z in match(d):
                txt = d[y:].text.split()[0]
                i = txt.find('http')
                i = i+4 if i > -1 else 0
                j = txt.find('\n', i)
                pre, txt = '', txt[i:j if j > -1 else len(txt)]
                if txt.startswith('://'):
                    pre, txt = 'p', txt[3:]
                elif txt.startswith('s://'):
                    pre, txt = 's', txt[4:]

                txt = txt.replace('\\', '').strip()
                dat.append([ud['id'], pre, txt])

        df = data(dat, 'id http url')
        df.url = df.url.apply(clean)
        df.url = df.url.str.split('"', 1).str[0]
        df.url = df.url.str.extract('([\w_-]+\.\w+.*)$')
        df = df[df.url.notna()]
        df = df[df.url.str.contains("^[\w\-\._~:/\?#\[\]@!$&'\(\)\*\+,;=%{}|\^`]+$")]
        f = f'out/{dir}/match/urls/{f.split("/")[-1].replace(".spacy", ".csv")}'
        save(f, df)

    assert(n == stats[dir])

@app.command()
def terms(dir: str, tag: str, terms_f: str):
    trms, combos, raw = set(), [], []
    with open(terms_f) as f:
        for x in f:
            ands = []
            for and_ in x.split(';'):
                ands.append({x.strip() for x in and_.split(',')})
                trms.update(ands[-1])

            combos.append(ands)
            raw.append(x.strip())

    m = match = matcher.PhraseMatcher(nlp.vocab, attr='LOWER')
    m.add(tag, [nlp.make_doc(x) for x in trms])

    fs = glob.glob(f'{dir}/*.spacy')
    dir = os.path.normpath(dir).split('/')[-1]
    h = 'id matches combos'.split()
    os.makedirs('out/match/terms', exist_ok=True)
    with open(f'out/match/terms/{dir}.csv', 'w') as f:
        w = csv.writer(f)
        w.writerow(h)

    ids, dat = set(), []
    for f in tqdm(fs, total=len(fs)):
        bin = DocBin().from_disk(f)
        for d in bin.get_docs(nlp.vocab):
            id = d.user_data['id']
            ids.add(id)
            ms = match(d)
            if not ms: 
                continue

            ms = {d[bg:nd].text.lower() for _, bg, nd in ms}
            for i, x in enumerate(combos):
                ok = True
                for y in x:
                    if not ms & y: 
                        ok = False
                        break

                if ok:
                    dat.append([id, ','.join(ms), raw[i]])
                    if len(dat) and not len(dat) % 100000:
                        with open(f'out/match/terms/{dir}.csv', 'a') as f:
                            w = csv.writer(f)
                            w.writerows(dat)
                            dat = []

                    break

    with open(f'out/match/terms/{dir}.csv', 'a') as f:
        w = csv.writer(f)
        w.writerows(dat)

    assert(len(ids) == stats[dir])

if __name__ == '__main__':
    app()
