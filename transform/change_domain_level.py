import sys, os, numpy, csv, glob
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file, change_domain_level, nth_level_domain, domain_level
	
def change_domain_levels(src, dest, new_domain_level):
	print "Processing", src
	data = read_vm_file(src)
	clicks = {}
	for referrer, target, num_clicks in data:
		if referrer is None or target is None or num_clicks == -1\
		or domain_level(referrer) == 1 or domain_level(target) == 1:
			continue
		newr = change_domain_level(referrer, new_domain_level)
		newt = change_domain_level(target, new_domain_level)
		if newr not in clicks:
			clicks[newr] = {}
		if newt not in clicks[newr]:
			clicks[newr][newt] = 0
		clicks[newr][newt] += num_clicks
	
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer in clicks:
			for target in clicks[referrer]:
				writer.writerow([referrer, target, clicks[referrer][target]])

def worker(params):
	return change_domain_levels(*params)
		
def test():
	src = os.path.join(os.getenv("TD"), "vm", "test", "full-domain", "month", "2010", "02", "small-2010-02.txt")
	dest = os.path.join(os.getenv("TD"), "vm", "test", "output", "2010-02.txt")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	worker((src, dest, 3))

if __name__ == "__main__":
	test()
