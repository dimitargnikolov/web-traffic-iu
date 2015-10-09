import sys, os, numpy, csv, glob
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file, domain_level, nth_level_domain, normalize_url

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..', 'news'))
from filter_news_urls import EXCEPTION_PATTERNS, EXCEPTIONS, fnmatches_multiple

def should_skip_host(h):
	return h in EXCEPTIONS or fnmatches_multiple(EXCEPTION_PATTERNS, h)

def filter_news_junk(src, dest):
	print "Processing %s" % src
	data = read_vm_file(src)
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer, target, num_clicks in data:
			target = normalize_url(target)
			if not should_skip_host(target):
				writer.writerow([referrer, target, num_clicks])

def worker(params):
	return filter_news_junk(*params)

def run_in_parallel():
	params = []
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "news-only", "full-domain", "month", "filtered-referrers--filtered-targets--no-iu--no-unwanted", "*", "*", "*.txt"))
	dest_dir = os.path.join(os.getenv("TD"), "vm", "news-only", "full-domain", "month", "filtered-referrers--filtered-targets--no-iu--no-unwanted--no-junk")
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
	dest = os.path.join(os.getenv("TD"), "vm", "test", "output", "2007-05--no-iu.txt")
	worker((src, dest))

if __name__ == "__main__":
	#test()
	run_in_parallel()
