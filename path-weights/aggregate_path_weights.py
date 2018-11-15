import os
from operator import itemgetter

BASE_DATA_DIR = os.path.join('/', 'N', 'dc2', 'projects', 'filter_bubble', 'projects', 'web-traffic', 'data')

DATA_DIR = os.path.join(BASE_DATA_DIR, 'paths')
DEST = os.path.join(BASE_DATA_DIR, 'all-paths.tsv')

def read_paths():
    paths = {}
    for filename in os.listdir(DATA_DIR):
        print('Reading {}'.format(filename))
        with open(os.path.join(DATA_DIR, filename), 'rb') as f:
            f.readline() # skip header
            for line in f:
                row = [t.strip() for t in line.split(b'\t')]
                referrer = row[0]
                target = row[1]
                weight = int(row[2])
                if referrer not in paths:
                    paths[referrer] = {}
                if target not in paths[referrer]:
                    paths[referrer][target] = 0
                paths[referrer][target] += weight
    return paths

if __name__ == '__main__':
    print('Reading paths.')
    paths = read_paths()
    print(len(paths))

    print('Transforming paths.')
    tuples = []
    for referrer in paths:
        for target in paths[referrer]:
            tuples.append((referrer, target, paths[referrer][target]))

    print('Sorting paths.')
    sorted_paths = sorted(tuples, key=itemgetter(2), reverse=True)

    print('Writing paths.')
    with open(DEST, 'wb') as f:
        f.write(b'referrer\ttarget\tpath weight\n')
        for t in sorted_paths:
            f.write(b'\t'.join([ti if isinstance(ti, bytes) else str(ti).encode() for ti in t]) + b'\n')
