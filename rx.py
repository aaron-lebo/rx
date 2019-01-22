import argparse
import collections
import csv
import json
import lzma
import sys

parser = argparse.ArgumentParser()
parser.add_argument('command', choices=['count-subs', 'sample'])
parser.add_argument('files', nargs='*')
args = parser.parse_args()

total = 0#int(sys.argv[2]) if len(sys.argv) > 2 else 0
counts, things = collections.OrderedDict([('*', collections.Counter())]),  []
for file in args.files:
    count = collections.Counter()
    with lzma.open(file) as f:
        try:
            for n, line in enumerate(f):
                thing = json.loads(line)
                count[thing['subreddit']] += 1
                things.append(thing)
                if total and n + 1 == total:
                    break
        except EOFError:
            pass
        finally:
            counts[file] = count
            counts['*'] += count

if args.command == 'count-subs':
    most_common = counts['*'].most_common()
    with open('subreddit-counts.csv', 'w') as f:
        w = csv.writer(f)
        w.writerow(['subreddit'] + [file.split('/')[-1] for file in counts])
        w.writerow(['*'] + [sum(counts[file].values()) for file in counts])
        for k, _ in most_common:
            w.writerow([k] + [counts[file].get(k, 0) for file in counts])
else:
    json.dump(things, sys.stdout, indent=2)
