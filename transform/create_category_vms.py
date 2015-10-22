import sys, os, numpy, csv, glob
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import read_vm_file, domain_level, nth_level_domain

sys.path.append(os.path.join(os.getenv("TC"), "filter"))
from filter_referrers import SEARCH_ENGINES, SOCIAL_MEDIA_PLATFORMS, EMAIL_PLATFORMS

def domain_levels(hosts):
	dls = set()
	for host in hosts:
		dl = domain_level(host)
		if dl not in dls:
			dls.add(dl)
	return dls

def is_member(h, hosts):
	if len(hosts) == 1 and hosts[0] == '':
		return h == ''
	else:
		dls = domain_levels(hosts)
		for dl in dls:
			if nth_level_domain(h, dl) in hosts:
				return True
	return False

def create_categories(src, dest, hosts):
	print "Processing %s" % dest
	data = read_vm_file(src)
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer, target, num_clicks in data:
			if is_member(referrer, hosts):
				writer.writerow([referrer, target, num_clicks])

def worker(params):
	return create_categories(*params)

def run_in_parallel():
	params = []
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "level3-domain", "month", "filtered", "*", "*", "*.txt"))
	dest_dir = os.path.join(os.getenv("TD"), "vm", "level3-domain", "month", "categories")
	
	cats = [
		("search", SEARCH_ENGINES),
		("social", SOCIAL_MEDIA_PLATFORMS),
		("email", EMAIL_PLATFORMS)
	]

	for category, hosts in cats:
		for f in files:
			remainder, filename = os.path.split(f)
			remainder, month = os.path.split(remainder)
			_, year = os.path.split(remainder)
			destf = os.path.join(dest_dir, category, year, month, filename)
			if not os.path.exists(os.path.dirname(destf)):
				os.makedirs(os.path.dirname(destf))
			params.append((f, destf, hosts))
	
	p = Pool(processes=16)
	p.map(worker, params)
		
def test():
	src = os.path.join(os.getenv("TD"), "vm", "test", "level3-domain", "month", "2007", "05", "2007-05.txt")
	dest = os.path.join(os.getenv("TD"), "vm", "test", "output", "2007-05-social-media.txt")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	worker((src, dest, SOCIAL_MEDIA_PLATFORMS))

if __name__ == "__main__":
	run_in_parallel()
