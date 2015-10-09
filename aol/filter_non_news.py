import sys, os, numpy, csv, glob
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import read_aol_data, domain_level, nth_level_domain

NEWS = frozenset([s.strip() for s in open(os.path.join(os.getenv("TD"), "news-urls-filtered.txt"), 'r').readlines()])

domain_levels = set()
for host in NEWS:
	dl = domain_level(host)
	if dl not in domain_levels:
		domain_levels.add(dl)
DOMAIN_LEVELS = frozenset(domain_levels)

def should_skip_host(h):
	for dl in DOMAIN_LEVELS:
	   	if nth_level_domain(h, dl) in NEWS:
	   		return False
	return True

def filter_targets(src, dest):
	print "Processing %s" % src
	data = read_aol_data(src)
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		writer.writerow(['user', 'url'])
		for user, dest in data:
			if not should_skip_host(dest):
				writer.writerow([user, dest])

def worker(params):
	return filter_targets(*params)

def run_in_parallel():
	files = glob.glob(os.path.join(os.getenv("TD"), "aol", "normalized", "*"))
	dest_dir = os.path.join(os.getenv("TD"), "aol", "news-only")
	if not os.path.exists(dest_dir):
		os.makedirs(dest_dir)
	params = [(f, os.path.join(dest_dir, os.path.split(f)[1])) for f in files]
	print len(params)
	p = Pool(processes=16)
	results = p.map(worker, params)

if __name__ == "__main__":
	run_in_parallel()
