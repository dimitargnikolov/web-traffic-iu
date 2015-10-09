import sys, os, numpy, csv, glob
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file, domain_level, nth_level_domain, fnmatches_multiple, is_ip_address, normalize_url

UNWANTED_DOMAINS = frozenset([
	'socialmedia.com',
	'kitnmedia.com',
	'insightexpressai.co',
	'mediaplex.com',
	'cubics.com',
	'rockyou.com',
	'fimserve.com',
	'addthis.com',
	'advertising.com',
	'doubleclick.net',
	'adrevolver.com',
	'precisionclick.com',
	'trafficmp.com',
	'atwola.com',
	'adbrite.com',
	'googlesyndication.com',
	'questionmarket.com',
	'interclick.com',
	'bidsystem.com',
	'quizapps.com',
	'yieldmanager.com',
	'trafficmp.com',
	'11piecesofflare.com',
	'presidiomedia.com',
	'addynamix.com',
	'opt.fimserve.com',
	'adserver.yahoo.com',
	'contextweb.com',
	'adsonar.com',
	'intellitxt.com',
	'rmxads.com',
	'eyewonder.com',
	'cpxinteractive.com',
	'ad.naver.com',
	'andomedia.com',
	'adjuggler.com',
	'ad.jamster.com',
	'serving-sys.com',
	'ads.revsci.net',
	'recaptcha.net',
	'myspacecdn.com',
	'snocap.com',
	'fbcdn.net',
	'atdmt.com',
	'gvt0.com',
	'gstatic.com',
	'ggpht.com',
	'akam-ai.net',
	'youtube-nocookie.com',
	'ytimg.com',
	'imrworldwide.com',
	'applatform.com',
	'desktopfw.weather.com',
	'presence.userplane.com',
	'userplane.com',
	'stationdata.wunderground.com',
	'wxbug.com',
	'ttsite.com',
	'safebrowsing-cache.google.com',
	'suggestqueries.google.com',
	'channel.facebook.com',
	'presence.facebook.com',
	'ebayrtm.com',
	'context3.kanoodle.com',
	'l.google.com',
	'sb.google.com',
	'mqcdn.com',
	'cdn-aimtoday.aol.com',
	'hs.facebook.com',
	'ru4.com',
	'toolbarqueries.google.com',
	'toolbar.yahoo.com',
	'assoc-amazon.com',
	'yimg.com',
	'channel.aol.com',
	'toolbarqueries.google.com',
	's3.amazonaws.com',
	'adcontent.videoegg.com',
	'insightexpressai.com',
	'bit.ly',
	'farmville.com',
	'adknowledge.com',
	'app2.yoville.com',
	'locate.videoegg.com',
	'scrabulousapps.com',
	'fimnetwork.com',
	'visiblemeasures.com',
	'sometrics.com',
	'tscapeplay.com',
	'msplinks.com',
	'media.imeem.com',
	'petswf.bunnyherolabs.com',
	'coolchaser.com',
	'game.playfish.com',
	'citi.bridgetrack.com',
	'chainn.mepopular.com',
	'flair.nliven.com',
	'funsocialapps.com',
	'optimized-by.rubiconproject.com',
	'slide.com',
	'cache.googlevideo.com',
	'oodleimg.com',
	'turn.com',
	'tradescape.biz',
	'rover.ebay.com',
	'clickfrom.buy.com',
	'facebook.livingsocial.com',
	'facebook.mafiawars.com',
	'piratewarsonline.com',
	'whos.amung.us',
	'eamobile.com',
	'brightcove.com',
	'personalweb.com',
	'tinyurl.com',
	'bit.ly',
	'adshuffle.com', 
	'modmyprofile.com',
	'fbexchange.com',
	'fbgaming.com',
	'mmismm.com',
	'adsfac.us',
	'googleadservices.com',
	'adbureau.net',
	'yourownapps.com',
	'adparlor.com',
	'adnectar.com',
	'fastclick.net',
	'constantcontact.com',
	'thinktarget.com',
	'clearspring.com',
	'yontoo.com',
	'2mdn.net',
	'dlqm.net',
	'mxrs.com',
	'mxrs.net',
	'statcounter.com',
	'videoegg.com',
	'honestybox.com',
	'mrninja.com',
	'joyent.us',
	'playdom.com',
	'6waves.com',
	'mookie1.com',
	'xanga.com',
	'sochr.com',
	'specificclick.net',
	'fwmrm.net',
	'edgecastcdn.net',
	'slashkey.com',
	'contextuads.com',
	'popcap.com',
	'contextuads.com',
	'valuead.com',
	'coolonline.com'
])

UNWANTED_PATTERNS = frozenset([
	"channel*.facebook.com",
	"khm*.google.com",
	"kh*.google.com",
	"mt*.google.com",
	"clients*.google.com",
	"feeds.*",
	"ads.*",
	"ad.*",
	"rss.*",
	"rssfeeds.*",
	"deco*.slide.com",
	"deco*.slides.com",
	"*.zynga.com",
	"fb.*.com",
	"app.*.com",
	"apps.*.com",
	"img.*.com",
	"static.*",
	"lax-v*",
	"syndication.*",
	"*nyadmcncserve*",
	"facebook.*"
])

domain_levels = set()
for host in UNWANTED_DOMAINS:
	dl = domain_level(host)
	if dl not in domain_levels:
		domain_levels.add(dl)
DOMAIN_LEVELS = frozenset(domain_levels)

def should_skip_host(h):
	if is_ip_address(h):
		return True
	elif domain_level(h) <= 1:
		return True
	for dl in DOMAIN_LEVELS:
	   	if nth_level_domain(h, dl) in UNWANTED_DOMAINS:
   			return True
	return fnmatches_multiple(UNWANTED_PATTERNS, h)

def remove_unwanted(src, dest):
	print "Processing %s" % src
	data = read_vm_file(src)
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer, target, num_clicks in data:
			r = normalize_url(referrer)
			t = normalize_url(target)
			if not should_skip_host(t):
				writer.writerow([r, t, num_clicks])

def worker(params):
	return remove_unwanted(*params)

def run_in_parallel():
	params = []
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "full-domain", "month", "filtered-referrers--filtered-targets--no-iu", "*", "*", "*.txt"))
	dest_dir = os.path.join(os.getenv("TD"), "vm", "full-domain", "month", "filtered-referrers--filtered-targets--no-iu--no-unwanted")
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
	dest = os.path.join(os.getenv("TD"), "vm", "test", "output", "2007-05--no-ads.txt")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	worker((src, dest))

if __name__ == "__main__":
	#test()
	run_in_parallel()
