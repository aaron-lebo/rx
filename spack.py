from datetime import datetime
import glob
import json
import subprocess
import os

import pandas as pd
import spacy
from tqdm.auto import tqdm
import typer

app = typer.Typer()

stats = pd.read_csv('stats.csv')
ext = stats.file.str.split('.').str
stats.file, stats['ext'] = ext[0].str.lower(), ext[1]

ks = 'id created_utc edited retrieved_on author subreddit'.split()
ks_com = 'link_id parent_id body'.split()
ks_sub = 'url title selftext'.split()

def split(file: str):
    f = os.path.normpath(file.lower()).split(os.sep)[-1]
    assert(f[:3] in ('rc_', 'rs_'))
    return f[:3], f

def save(dat, ks, file, day):
    df = pd.DataFrame(dat, columns=ks).set_index('id')
    df.created_utc = pd.to_datetime(df.created_utc, unit='s')
    df.loc[df.edited==False, 'edited'] = None
    df.edited = pd.to_datetime(df.edited, unit='s')
    df.retrieved_on = pd.to_datetime(df.retrieved_on, unit='s')

    pq = f'out/{file}-{day:02}.pq'
    if os.path.exists(pq):
        df = pd.concat([pd.read_parquet(pq), df])

    df.sort_values(['id']).to_parquet(pq)

@app.command()
def pack(file: str):
    pre, file1 = split(file)
    what, ks1 = ('comments', ks+ks_com) if pre == 'rc_' else ('submissions', ks+ks_sub)
    stat = stats[stats.file==file1].iloc[0]

    subprocess.run(['wget', f'https://files.pushshift.io/reddit/{what}/{file}.{stat.ext}'])
    if stat.ext == 'bz2':
        subprocess.run(['bzip2', '-dv', file+'.bz2'])
    else:
        subprocess.run(['zstd', '-d', f'{file}.{stat.ext}', '--long=31', '--rm'])

    with open(file) as f:
        n = sum(1 for x in f)
        assert(n == stat.num)
        f.seek(0)
        os.makedirs('out/', exist_ok=True)
        
        dat, day = [], None
        for i, x in tqdm(enumerate(f), total=n):
            x = json.loads(x)
            dt = datetime.utcfromtimestamp(int(x['created_utc']))
            if day and day != dt.day:
                save(dat, ks1, file1, day)
                dat = []

            dat.append([x.get(k) for k in ks1])
            day = dt.day

        save(dat, ks1, file1, day)
        assert(n == i+1)

    os.remove(file)

nlp = spacy.load('en_core_web_lg')

def load(x, cols):
    return '\n'.join(x[1][cols[1:]]), x[0]

@app.command()
def process(path: str):
    for file in sorted(glob.glob(path)):
        pre, _ = split(file)
        cols = ['id', 'body'] if pre == 'rc_' else ['id', 'title', 'selftext'] 
        df = pd.read_parquet(file, columns=cols)
        p = nlp.pipe((load(x, cols) for x in df.iterrows()), as_tuples=True)
        bar = tqdm(p, total=len(df))
        bin = spacy.tokens.DocBin(store_user_data=True)
        bar.set_description(file)
        for d, id in bar:
            d.user_data['id'] = id
            bin.add(d)

        f = file.replace('.pq', '.spacy')
        if os.path.isfile(f):
            raise FileExistsError

        bin.to_disk(f)

if __name__ == '__main__':
    app()
