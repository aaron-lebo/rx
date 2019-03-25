import argparse
import collections
import csv
import json
import lzma
import random
import sys

parser = argparse.ArgumentParser()
parser.add_argument('-n', type=int)
parser.add_argument('-s', '--sub')
parser.add_argument('--exclude', dest='exclude', action='store_true')
parser.add_argument('command', choices=['count-subs', 'sample'])
parser.add_argument('files', nargs='*')
args = parser.parse_args()

counts, things = collections.OrderedDict([('*', collections.Counter())]), []
for file in args.files:
    count = collections.Counter()
    with lzma.open(file) as f:
        try:
            for line in f:
                thing = json.loads(line)
                count[thing['subreddit']] += 1
                things.append(thing)
        except EOFError:
            pass
        finally:
            counts[file] = count
            counts['*'] += count

if args.command == 'count-subs':
    with open('subreddits.csv', 'w') as f:
        w = csv.writer(f)
        w.writerow(['subreddit'] + [file.split('/')[-1] for file in counts])
        w.writerow(['*'] + [sum(vals.values()) for vals in counts.values()])
        for k, _ in counts['*'].most_common():
            w.writerow([k] + [vals.get(k, 0) for vals in counts.values()])
else:
    if args.sub:
        things = [t for t in things if t['subreddit'] in args.sub]
    elif args.exclude:
        with open('excluded.txt') as f:
            excluded = [sub.strip() for sub in f]
        things = [t for t in things if not t['subreddit'] in excluded and counts['*'][t['subreddit']] > 1]
    json.dump(random.sample(things, args.n) if args.n and args.n <= len(things) else things, sys.stdout, indent=2)
    print()
