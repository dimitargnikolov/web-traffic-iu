import sys, os, fnmatch

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import domain_level, nth_level_domain, is_exception, parents, normalize_url, fnmatches_multiple

def fnmatches_multiple(patterns, s):
	for p in patterns:
		if fnmatch.fnmatch(s, p):
			return True
	return False


sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..', 'filter'))
from filter_referrers import EMAIL_PLATFORMS, SOCIAL_MEDIA_PLATFORMS, SEARCH_ENGINES, NEWS_RECOMMENDERS, BLOGGING_PLATFORMS


EXCEPTIONS = frozenset([
	'desktopapps.bbc.co.uk',
	'newsrss.bbc.co.uk',
	'catalog.video.msn.com',
	'graphics8.nytimes.com',
	'public-xml.nytimes.com',
	'newsimg.bbc.co.uk',
	'media-ori.msnbc.msn.com',
	'webfarm.tpa.foxnews.com'
])
EXCEPTION_PATTERNS = [
	'img*.catalog.video.msn.com',
	'*.video.msn.com',
	'feeds.*',
	'ads.*',
	'rss.*',
	'rssfeeds.*',
	'pheedo*.msnbc.msn.com',
	'graphics*.nytimes.com',
	'*.edu',
	'*.edu.*'
]
UNWANTED_URLS = frozenset().union(EMAIL_PLATFORMS).union(SOCIAL_MEDIA_PLATFORMS).union(SEARCH_ENGINES).union(NEWS_RECOMMENDERS).union(BLOGGING_PLATFORMS).union(EXCEPTIONS)

def prune_news_dataset(news_sources_file):
	f = open(news_sources_file, 'r')
	news_urls = set()
	for line in f:
		host = line.strip().split('/')[0]
		
		host = normalize_url(host)

		if host in news_urls or host in UNWANTED_URLS or fnmatches_multiple(EXCEPTION_PATTERNS, host):
			continue

		news_urls.add(host)
	
	for host in sorted(list(news_urls)):
		disregard = False
		for parent in parents(host):
			if parent in news_urls or parent in UNWANTED_URLS:
				disregard = True
				break
		if not disregard:
			print host

def main():
	prune_news_dataset(os.path.join(os.getenv("TD"), "news-urls.txt"))

def test():
	import doctest
	doctest.testmod()

if __name__ == "__main__":
	main()
