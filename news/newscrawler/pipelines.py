from scrapy.exceptions import DropItem
		
class ValidNewsSourcePipeline(object):
	def __init__(self):
		self.seen_hosts = set()
		
	def process_item(self, item, spider):
		if item['url'] is None or not item['url'].startswith('http://'):
			raise DropItem("Skipping link: %s" % item['url'])
		else:
			_, url = item['url'].split('://')
			host = url.split('/')[0]

			if host.startswith('www.'):
				host = host[4:]
			elif host.startswith('www2.') or host.startswith('www3.'):
				host = host[5:]

			if 'dmoz.org' in host:
				raise DropItem("Skipping dmoz.org url: %s" % item['url'])
			elif host in self.seen_hosts:
				raise DropItem("Duplicate item: %s" % item['url'])
			else:
				self.seen_hosts.add(host)
				return item
