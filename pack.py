import argparse
from datetime import datetime
import json
import sqlite3
import time

p = argparse.ArgumentParser()
p.add_argument('file', nargs='+')
args = p.parse_args()

def get_db(f, what):
    ks = 'id,created_utc integer,edited integer,retrieved_on integer,'
    subm_ks = ks + 'author,subreddit,url,title,selftext'
    comm_ks = ks + 'link_id,parent_id,author,subreddit,body'
    ks = comm_ks if what == 'comments' else subm_ks
    cn = sqlite3.connect(f'{f}.db')
    cr = cn.cursor()
    try:
        unique = 'constraint unique_id unique (id)'
        cr.execute(f'create table {what}({ks},{unique})')
        cr.execute(f'create index idx_{what}_id on {what}(id)')
    except sqlite3.OperationalError:
        pass
    return [k.split()[0] for k in ks.split(',')], cn, cr

files = args.file
nfiles = len(files)

def save(i, t, what, f, cn, cr, n, xs, j, end='\n'):
    t = datetime.now() - t
    print(f'{i+1:2} {nfiles} {f.name} {n:12,} {(j+1)/n*100:6.2f}% {t}', end=end)
    cr.executemany(f'insert into {what} values (?, ?, ?, ?, ?, ?, ?, ?, ?)', xs)
    cn.commit()

for i, f in enumerate(files):
    t = datetime.now()
    fname = f.split('/')[-1][:7].lower()
    what = 'comments' if fname.startswith('rc') else 'submissions'
    f = open(f)
    ks, cn, cr = get_db(fname, what)
    n = sum(1 for x in f)
    f.seek(0)

    xs = []
    for j, line in enumerate(f):
        if not line: 
            continue

        x = json.loads(line)
        xs.append([x.get(k) for k in ks])
        if not j % 100000: 
            save(i, t, what, f, cn, cr, n, xs, j, '\r')
            xs = []

    save(i, t, what, f, cn, cr, n, xs, j)
    cn.close()
