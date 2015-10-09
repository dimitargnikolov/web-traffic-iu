import sys, os, numpy, csv, glob
from multiprocessing import Pool

from filter_referrers import WANTED_DOMAINS as UNWANTED_DOMAINS, DOMAIN_LEVELS

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file, domain_level, nth_level_domain

def should_skip_host(h):
	for dl in DOMAIN_LEVELS:
	   	if nth_level_domain(h, dl) in UNWANTED_DOMAINS:
	   		return True
	return False

def filter_targets(src, dest):
	print "Processing %s" % src
	data = read_vm_file(src)
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer, target, num_clicks in data:
			if not should_skip_host(target):
				writer.writerow([referrer, target, num_clicks])

def worker(params):
	return filter_targets(*params)

def run_in_parallel():
	params = []
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "full-domain", "month", "filtered-referrers", "*", "*", "*.txt"))
	dest_dir = os.path.join(os.getenv("TD"), "vm", "full-domain", "month", "filtered-referrers--filtered-targets")
	for f in files:
		remainder, filename = os.path.split(f)
		remainder, month = os.path.split(remainder)
		_, year = os.path.split(remainder)
		destf = os.path.join(dest_dir, year, month, filename)
		if not os.path.exists(os.path.dirname(destf)):
			os.makedirs(os.path.dirname(destf))
		params.append((f, destf))
	
	p = Pool(processes=10)
	results = p.map(worker, params)
		
def test():
	src = os.path.join(os.getenv("TD"), "vm", "test", "full-domain", "month", "2007", "05", "2007-05.txt")
	dest = os.path.join(os.getenv("TD"), "vm", "test", "output", "2007-05--filtered-targets.txt")
	worker((src, dest))

if __name__ == "__main__":
	#test()
	run_in_parallel()
