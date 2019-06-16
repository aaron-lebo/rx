import argparse
import bz2
import collections
import datetime
import glob
import json
import lzma
import os.path
import re
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument('command', choices='submissions comments'.split())
parser.add_argument('-u', action='store_true')
parser.add_argument('dir')
args = parser.parse_args()

crimea = ('crimea', 'crimean', 'crimeans')
putin = ('putin',)
russia = ('russia', 'russian', 'russians')
ukraine = ('ukraine', 'ukrainian', 'ukrainians')
query = [(ukraine, russia), (crimea, russia), (ukraine, putin), (crimea, putin)]

db_fields = dict(
    comments = 'id author subreddit created_utc retrieved_on edited body link_id parent_id matches'.split(),
    submissions = 'id author subreddit created_utc retrieved_on edited title url selftext matches'.split(),
)

con = sqlite3.connect('rx.db')
try:
    for k, v in db_fields.items():
        con.execute(f'create table {k}({", ".join([f + " timestamp" if 2 < n < 6 else f for n, f in enumerate(v)])})')
    con.execute('create table results(file, counts json, matches integer, relatives integer, total integer)')
except:
    pass

files = {row[0] for row in con.execute('select file from results where relatives > 0')}
ids = {row[0] for row in con.execute('select id from comments')}
ids.update(row[0] for row in con.execute('select id from submissions'))
link_ids = {row[0] for row in con.execute('select link_id from comments')}

def print_status(file, start, objs, n, end='\n'):
    print(file, datetime.datetime.now() - start, len(objs), 'selected /', n, 'total', end=end)

fields = db_fields[args.command]
comments = args.command == 'comments'
submissions = not comments

pattern = re.compile('\W+')

def add_obj(objs, obj, cnt, key):
    objs.append([obj.get(k, '') for k in fields])
    ids.add(obj['id'])
    cnt[key] += 1

for filepath in glob.glob(os.path.join(args.dir, 'RC_*' if comments else 'RS_*')):
    file = os.path.basename(filepath)
    print(file, end='\r')
    if file in files:
        print(file, 'already done')
        continue

    start = datetime.datetime.now()
    open_ = bz2.BZ2File if file.endswith('.bz2') else lzma
    objs, cnt = [], collections.Counter()
    with open_(filepath) as f:
        for n, line in enumerate(f):
            if not (n + 1) % 10000:
                print_status(file, start, objs, n + 1, '\r')

            obj = json.loads(line)
            id = ('t1_' if comments else 't3_') + obj['id']
            obj['id'] = id
            if id in ids:
                continue

            if submissions and args.u:
                if id in link_ids:
                    add_obj(objs, obj, cnt, 'relatives')
                continue

            text = obj['body'] if comments else ' '.join(obj[k] for k in ('title', 'url', 'selftext'))
            words = pattern.split(text.lower())
            for or_clause in query:
                matches = []
                for and_clause in or_clause:
                    for keyword in and_clause:
                        if keyword in words:
                            matches.append(keyword)
                            break
                if matches and len(matches) == len(or_clause):
                    obj['matches'] = ', '.join(matches)
                    add_obj(objs, obj, cnt, or_clause)
                    break
            if comments and not obj.get('matches') and (obj['parent_id'] in ids or obj['link_id'] in ids):
                add_obj(objs, obj, cnt, 'relatives')

    print_status(file, start, objs, n + 1)
    if comments or not args.u:
        match_cnt = {str(k): v for k, v in cnt.items() if not k == 'relatives'}
        con.execute('insert into results values (?, ?, ?, ?, ?)',
            (file, json.dumps(match_cnt), sum(match_cnt.values()), cnt['relatives'], n + 1))
    else:
        con.execute('update results set relatives = ? where file = ?', (cnt['relatives'], file))
    con.executemany(f'insert into {args.command} values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', objs)
    con.commit()

con.close()
