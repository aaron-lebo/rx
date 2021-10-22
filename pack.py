import argparse
from datetime import datetime
import json
import sqlite3
import time

p = argparse.ArgumentParser()
p.add_argument('file', nargs='+')
args = p.parse_args()

ks = 'id,created_utc int,edited int,retrieved_on int,cat int,'
comms = ('comments', ks+'link_id,parent_id,author,subreddit,body')
subms = ('submissions', ks+'author,subreddit,url,title,selftext')

cn = sqlite3.connect('rx.db')
cr = cn.cursor()

for what, ks1 in [comms, subms]:
    try:
        unique = 'constraint unique_id unique (id)'
        cr.execute(f'create table {what}({ks1},{unique})')
        cr.execute(f'create index idx_{what}_id on {what}(id)')
        cr.execute(f'create index idx_{what}_cat on {what}(cat)')
    except sqlite3.OperationalError:
        pass

nfiles = len(args.file)
for i, f in enumerate(args.file):
    start = t = datetime.now()

    what, ks = comms if f.split('/')[-1].lower().startswith('rc') else subms
    ks = [k.split()[0] for k in ks.split(',')]

    f = open(f)
    n = sum(1 for x in f)
    f.seek(0)

    xs = []
    for j, line in enumerate(f):
        if not line: 
            continue

        x = json.loads(line)
        xs.append([x.get(k) for k in ks])
        t1 = datetime.now()
        if n == j+1 or (t1-t).total_seconds() > 1:
            print(f'{i+1:2}  {nfiles}  {f.name}  {n:12,}  {(j+1)/n*100:6.2f}%  {t1-start}', end='\r')
            cr.executemany(f'insert into {what} values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', xs)
            cn.commit()
            xs, t = [], t1
