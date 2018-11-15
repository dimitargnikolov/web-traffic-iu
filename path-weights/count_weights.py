import os

if __name__ == '__main__':
    base_dir = '/N/dc2/projects/filter_bubble/projects/web-traffic/data'
    src = os.path.join(base_dir, 'all-paths.tsv')
    dest = os.path.join(base_dir, 'all-paths-counts.tsv')
    weights = {}
    
    with open(src, 'rb') as f:
        f.readline() # skip header
        for line in f:
            row = line.split(b'\t')
            w = int(row[2])
            if w not in weights:
                weights[w] = 0
            weights[w] += 1

    with open(dest, 'w') as f:
        f.write('weight\tcount\n')
        for w in sorted(weights.keys()):
            f.write('{}\t{}\n'.format(w, weights[w]))
            
