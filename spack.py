from datetime import datetime
import json 
import subprocess
import os

import pandas as pd
from tqdm.auto import tqdm
import typer

app = typer.Typer()

stats = pd.read_csv('stats.csv')
ext = stats.file.str.split('.').str
stats.file, stats['ext'] = ext[0].str.lower(), ext[1]

ks = 'id created_utc edited retrieved_on author subreddit'.split()
ks_com = 'link_id parent_id body'.split()
ks_sub = 'url title selftext'.split()

def save(dat, ks, file, day):
    df = pd.DataFrame(dat, columns=ks).set_index('id')
    df.created_utc = pd.to_datetime(df.created_utc, unit='s')
    df.loc[df.edited==False, 'edited'] = None
    df.edited = pd.to_datetime(df.edited, unit='s')
    df.retrieved_on = pd.to_datetime(df.retrieved_on, unit='s')

    pq = f'out/{file}-{day:02}.pq'
    if os.path.exists(pq):
        df = pd.concat([pd.read_parquet(pq), df])

    df.to_parquet(pq)

@app.command()
def pack(file: str):
    file1 = file.lower()
    pre = file1[:3]
    assert(pre in ('rc_', 'rs_'))

    if pre == 'rc_':
        subprocess.run(['wget', f'https://files.pushshift.io/reddit/comments/{file}.{stat.ext}'])
        ks1 = ks+ks_com
    else:
        subprocess.run(['wget', f'https://files.pushshift.io/reddit/submissions/{file}.zst'])
        ks1 = ks+ks_sub

    stat = stats[stats.file==file1].iloc[0]
    if stat.ext == 'bz2':
        subprocess.run(['bzip2', '-dv', file+'.bz2'])
    else:
        subprocess.run(['zstd', '-d', f'{file}.{stat.ext}', '--long=31'])

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

if __name__ == '__main__':
    app()
