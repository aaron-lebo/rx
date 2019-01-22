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
parser.add_argument('command', choices=['count-subs', 'sample'])
parser.add_argument('files', nargs='*')
args = parser.parse_args()

total = 0#int(sys.argv[2]) if len(sys.argv) > 2 else 0
counts, things = collections.OrderedDict([('*', collections.Counter())]), []
for file in args.files:
    count = collections.Counter()
    with lzma.open(file) as f:
        try:
            for n, line in enumerate(f):
                thing = json.loads(line)
                sub = thing['subreddit']
                if args.sub and not sub in args.sub:
                    continue
                count[sub] += 1
                things.append(thing)
                if total and n + 1 == total:
                    break
        except EOFError:
            pass
        finally:
            counts[file] = count
            counts['*'] += count

if args.command == 'count-subs':
    with open('subreddit-counts.csv', 'w') as f:
        w = csv.writer(f)
        w.writerow(['subreddit'] + [file.split('/')[-1] for file in counts])
        w.writerow(['*'] + [sum(vals.values()) for vals in counts.values()])
        for k, _ in counts['*'].most_common():
            w.writerow([k] + [vals.get(k, 0) for vals in counts.values()])
else:
    json.dump(random.sample(things, args.n) if args.n and args.n <= len(things) else things, sys.stdout, indent=2)
