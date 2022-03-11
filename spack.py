import glob
import json 
import os

import pandas as pd
import spacy
from spacy.tokens import DocBin
from tqdm.auto import tqdm
import typer

app = typer.Typer()
Op = typer.Option
nlp = spacy.load('en_core_web_lg')

stats = pd.read_csv('stats.csv')
stats.file = stats.file.str.lower()
stats = dict(stats.values)

@app.command()
def count(dir: str, check=True):
    fs = glob.glob(f'{dir}/*.spacy')
    n, ids = 0, set()
    for f in tqdm(fs, total=len(fs)):
        bin = DocBin().from_disk(f)
        n += len(bin) 
        if check:
            df = pd.read_csv(f.replace('.spacy', '.csv'))
            ids.update(df.id)

    if check:
        file = os.path.normpath(dir).split('/')[-1]
        print(n, len(ids), stats[file])
        assert(n == len(ids) == stats[file])
    return n

ks_ = 'id created_utc edited retrieved_on author subreddit'.split()
ks_com = 'link_id parent_id'.split()
ks_sub = ['url']

def load(s: str, pre: str):
    x = json.loads(s)
    if pre == 'rc_':
        return x['body'], x
    return '\n'.join([x['title'], x['selftext']]), x

@app.command()
def main(file: str, resume: bool = Op(False), start: int = Op(0), procs: int = Op(os.cpu_count()-1), full: bool = Op(True)):
    with open(file) as f:
        file = os.path.normpath(file).split('/')[-1].lower()
        pre = file[:3]
        ks = ks_+(ks_com if pre == 'rc_' else ks_sub)
        assert(pre in ('rc_', 'rs_'))

        n = sum(1 for x in f)
        assert(n == stats[file])
        f.seek(0)
        os.makedirs(f'out/{file}', exist_ok=True)
        if resume and not start:
            start = count(f'out/{file}', check=False)
        if start:
            n -= start
            for i, _ in enumerate(f):
                if i == start-1:
                    break

        p = (load(x, pre) for x in f)
        p = nlp.pipe(p, as_tuples=True, n_process=procs) if full else p
        dat, bin = [], DocBin(store_user_data=True)
        for i, (d, x) in enumerate(tqdm(p, total=n)):
            dat.append([x.get(k) for k in ks])
            if full:
                d.user_data['id'] = x['id']
                bin.add(d)

            if i and not i % 20000 or n == i+1:
                f = f'out/{file}/{x["id"]}.'    
                if os.path.isfile(f+'csv'):
                    raise FileExistsError
                if full:
                    if os.path.isfile(f+'spacy'):
                        raise FileExistsError

                    bin.to_disk(f+'spacy')
                    bin = DocBin(store_user_data=True)

                df = pd.DataFrame(dat, columns=ks).set_index('id')
                df.to_csv(f+'csv')
                dat = []
                
        assert(n == i+1)

if __name__ == '__main__':
    app()
