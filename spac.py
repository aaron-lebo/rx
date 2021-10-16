import argparse
from datetime import datetime
import sqlite3
import time

import spacy
from spacy.tokens import DocBin

p = argparse.ArgumentParser()
p.add_argument('file', nargs='+')

args = p.parse_args()
files = args.file
nfiles = len(files)

def get_db(f, what):
    ks = 'id,created_utc integer,edited integer,retrieved_on integer,'
    subm_ks = ks + 'author,subreddit,url,title,selftext'
    comm_ks = ks + 'link_id,parent_id,author,subreddit,body'
    ks = comm_ks if what == 'comments' else subm_ks
    cn = sqlite3.connect(f)
    cr = cn.cursor()
    return [k.split()[0] for k in ks.split(',')], cn, cr

nlp = spacy.load('en_core_web_lg')

for i, f in enumerate(files):
    t = datetime.now()
    fname = f.split('/')[-1].lower()
    what = 'comments' if fname.startswith('rc') else 'submissions'
    ks, cn, cr = get_db(fname, what)
    n = cr.execute(f'select count(*) from {what}').fetchone()[0]
    qy = cr.execute(f'select id, created_utc, title, selftext from {what} order by id asc')
    utc, bin = datetime(2001, 12, 31), DocBin() 
    for j, x in enumerate(qy.fetchall()):
        date, dt = utc.date(), datetime.fromtimestamp(x[1])
        doc = nlp(x[2])
        doc.user_data = dict(id=x[0])
        bin.add(doc)
        if date.year != 2001 and date != dt.date():
            bin.to_disk(f'{f[:3]}{date}.spacy')
            bin = DocBin() 
        if not j % 100:
            t1 = datetime.now() - t
            print(f'{i+1:2} {nfiles}  {f}  {n:12,}  {utc}  {(j+1)/n*100:6.2f}%  {t1}', end='\r')

        utc = dt

    t1 = datetime.now() - t
    print(f'{i+1:2} {nfiles}  {f}  {n:12,}  {utc}  {(j+1)/n*100:6.2f}%  {t1}')
    bin.to_disk(f'{f[:3]}{date}.spacy')


cn.close()
