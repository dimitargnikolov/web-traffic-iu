import sys, os, numpy, csv, glob
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import read_vm_file, domain_level, nth_level_domain

# not used but included in case we decide to analyze these sources again
WIKI_PLATFORMS = frozenset([
	'wikipedia.org',
	'wikimedia.org',
	'wictionary.org',
	'wikiquote.org',
	'wikibooks.org',
	'wikisource.org',
	'wikinews.org',
	'wikiversity.org',
	'wikidata.org',
	'wikivoyage.org',
	'wikimediafoundation.org'
])

# not used but included in case we decide to analyze these sources again
BLOGGING_PLATFORMS = frozenset([
	'xanga.com',
	'blogspot.com', 
	'blogger.com', 
	'wordpress.com',
	'wordpress.org',
	'tumblr.com',
	'typepad.com',
	'livejournal.com',
	'hubpages.com'
])

# not used but included in case we decide to analyze these sources again
NEWS_RECOMMENDERS = frozenset([
	'news.google.com',
	'news.yahoo.com',
	'news.aol.com'
])

EMAIL_PLATFORMS = frozenset([
	'umail.iu.edu',
	'imail.iu.edu',
	'webmail.iu.edu',
	'webmail.indiana.edu',
	'exchange.indiana.edu',
	'gmail.com',
	'mail.google.com',
	'ymail.com',
	'mail.yahoo.com',
	'hotmail.com',
	'mail.live.com',
	'webmail.aol.com',
	'hotmail.msn.com'
])

SOCIAL_MEDIA_PLATFORMS = frozenset([
	'facebook.com', 
	'myspace.com',
	'twitter.com', 
	'youtube.com',
	'plus.google.com',
	'linkedin.com',
	'pinterest.com',
	'reddit.com',
	'instagram.com',
	'vube.com'
])

SEARCH_ENGINES = frozenset([
	'search.aol.com',
	'google.com',
	'bing.com',
	'ask.com',
	'search.yahoo.com',
	'search.msn.com',
	'duckduckgo.com',
	'search.naver.com',
	'baidu.com'
])

WANTED_DOMAINS = frozenset().union(SEARCH_ENGINES).union(SOCIAL_MEDIA_PLATFORMS).union(EMAIL_PLATFORMS)

domain_levels = set()
for host in WANTED_DOMAINS:
	dl = domain_level(host)
	if dl not in domain_levels:
		domain_levels.add(dl)
DOMAIN_LEVELS = frozenset(domain_levels)

def should_skip_host(h):
	for dl in DOMAIN_LEVELS:
		if nth_level_domain(h, dl) in WANTED_DOMAINS:
			return False
	return True

def filter_referrers(src, dest):
	print "Processing %s" % src
	data = read_vm_file(src)
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer, target, num_clicks in data:
			if not should_skip_host(referrer):
				writer.writerow([referrer, target, num_clicks])

def worker(params):
	return filter_referrers(*params)

def run_in_parallel():
	params = []
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "news-only", "level3-domain", "month", "unfiltered", "*", "*", "*.txt"))
	dest_dir = os.path.join(os.getenv("TD"), "vm", "news-only", "level3-domain", "month", "filtered-referrers")
	for f in files:
		remainder, filename = os.path.split(f)
		remainder, month = os.path.split(remainder)
		_, year = os.path.split(remainder)
		destf = os.path.join(dest_dir, year, month, filename)
		if not os.path.exists(os.path.dirname(destf)):
			os.makedirs(os.path.dirname(destf))
		params.append((f, destf))
	
	p = Pool(processes=16)
	results = p.map(worker, params)
		
def test():
	src = os.path.join(os.getenv("TD"), "vm", "test", "full-domain", "month", "2007", "05", "2007-05.txt")
	dest = os.path.join(os.getenv("TD"), "vm", "test", "output", "2007-05--filtered-referrers.txt")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	worker((src, dest))

if __name__ == "__main__":
	run_in_parallel()
