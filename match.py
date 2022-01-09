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

def save(df, file):
    f = f'out/match/{os.path.split(file)[1].replace(".spacy", ".csv")}'
    if os.path.isfile(f):
        raise FileExistsError
    df.to_csv(f)

@app.command()
def urls(dir: str):
    fs = glob.glob(f'{dir}/*.spacy')
    m = match = matcher.Matcher(nlp.vocab)
    m.add('url',  [[{'LIKE_URL': True}], [{'LOWER': {'REGEX': r'\)\[http(.*)'}}]])
    os.makedirs('out/match', exist_ok=True)
    for i, f in enumerate(fs):
        bin = DocBin().from_disk(f)
        bar = tqdm(bin.get_docs(nlp.vocab), total=len(bin))
        bar.set_description(f'{i}/{len(fs)} {f}')
        dat = [] 
        for d in bar:
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
                dat.append([d.user_data['id'], pre, txt])

        df = data(dat, 'id http url')
        df.url = df.url.apply(clean)
        df = df[df.url.str.contains('[\w_-]+\.\w+.*$')]
        df.url = df.url.str.split('"', 1).str[0]
        df.url = df.url.str.extract('([\w_-]+\..+)$')
        df = df[~df.url.isna()]
        df = df[df.url.str.contains('^[\w\-\._~:/\?#\[\]@!$&\'\(\)\*\+,;=%{}|\^`]+$')]
        save(df, f)

@app.command()
def main(tag: str, terms_file: str, files: str):
    files = glob.glob(files)
    terms, combos, raw = set(), [], []
    with open(terms_file) as f:
        for x in f:
            ands = []
            for and_ in x.split(';'):
                ands.append({x.strip() for x in and_.split(',')})
                terms.update(ands[-1])

            combos.append(ands)
            raw.append(x.strip())

    m = matcher = matcher.PhraseMatcher(nlp.vocab, attr='LOWER')
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

    save(f, results, 'id matches combos')

if __name__ == '__main__':
    app()
