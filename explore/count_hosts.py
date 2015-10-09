import sys, os, glob, numpy
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import read_vm_file

sys.path.append(os.path.join(os.getenv("TC"), "filter"))
from filter_referrers import EMAIL_PLATFORMS, SOCIAL_MEDIA_PLATFORMS, SEARCH_ENGINES

def count_clicks_from_referrer(files, referrers):
	counts = {}
	for src in files:
		print "Processing", src
		data = read_vm_file(src)
		for referrer, target, num_clicks in data:
			if referrer in referrers:
				if not referrer in counts:
					counts[referrer] = 0
				counts[referrer] += 1
	return counts

def count_targets(files):
	targets = set()
	for src in files:
		print "Processing", src
		data = read_vm_file(src)
		for referrer, target, num_clicks in data:
			if target not in targets:
				targets.add(target)
	return len(targets)

def count_clicks(files):
	total = 0
	for src in files:
		print "Processing", src
		data = read_vm_file(src)
		for referrer, target, num_clicks in data:
			total += 1
	return total
	
def main():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "level3-domain", "month", "filtered", "*", "*", "*.txt"))
	
	clicks = count_clicks_from_referrer(files, set().union(EMAIL_PLATFORMS).union(SOCIAL_MEDIA_PLATFORMS).union(SEARCH_ENGINES))
	email_clicks = {site: count for site, count in clicks.items() if site in EMAIL_PLATFORMS}
	social_clicks = {site: count for site, count in clicks.items() if site in SOCIAL_MEDIA_PLATFORMS}
	search_clicks = {site: count for site, count in clicks.items() if site in SEARCH_ENGINES}

	sorted_clicks = sorted(email_clicks.items(), key=lambda tupl: tupl[1], reverse=True)
	sorted_clicks.extend(sorted(social_clicks.items(), key=lambda tupl: tupl[1], reverse=True))
	sorted_clicks.extend(sorted(search_clicks.items(), key=lambda tupl: tupl[1], reverse=True))

	for url, count in sorted_clicks:
		print url, count
	
#	print count_targets(files)

def test():
	print count_clicks(os.path.join(os.getenv("TD"), "vm", "test", "level3-domain", "month", "2007", "05", "2007-05.txt"))

if __name__ == "__main__":
	#test()
	main()
