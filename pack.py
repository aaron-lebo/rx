import argparse
from datetime import datetime
import json
import sqlite3
import time

p = argparse.ArgumentParser()
p.add_argument('file', nargs='+')
args = p.parse_args()

ks = 'id,created_utc integer,edited integer,retrieved_on integer,'
comms = ('comments', ks+'link_id,parent_id,author,subreddit,body')
subms = ('subissions', ks+'author,subreddit,url,title,selftext')

cn = sqlite3.connect('rx.db')
cr = cn.cursor()

for what, ks1 in [comms, subms]:
    try:
        unique = 'constraint unique_id unique (id)'
        cr.execute(f'create table {what}({ks1},{unique})')
        cr.execute(f'create index idx_{what}_id on {what}(id)')
    except sqlite3.OperationalError:
        pass

nfiles = len(args.file)

for i, f in enumerate(args.file):
    t = datetime.now()
    what, ks = comms if f.split('/')[-1].lower().startswith('rc') else subms
    ks = ks.split(',')
    f = open(f)
    n = sum(1 for x in f)
    f.seek(0)

    xs = []
    for j, line in enumerate(f):
        if not line: 
            continue

        x = json.loads(line)
        xs.append([x.get(k) for k in ks])
        if (not j % 100000) or j+1 == n:
            t = datetime.now() - t
            print(f'{i+1:2} {nfiles} {f.name} {n:12,} {(j+1)/n*100:6.2f}% {t}', end='\r')
            cr.executemany(f'insert into {what} values (?, ?, ?, ?, ?, ?, ?, ?, ?)', xs)
            cn.commit()
            xs = []

print()
