from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector

from newscrawler.items import LinkItem

class NewsLinksSpider(BaseSpider):
	name = "NewsLinksSpider"
	allowed_domains = ["dmoz.org"]
	crawl_paths = [
		'/News/Internet_Broadcasts/',
		'/News/Magazines_and_E-zines/',
		'/News/Newspapers/',
		'/News/Internet_Broadcasts/Audio/',
		'/Arts/Television/News/',
		'/News/Analysis_and_Opinion/',
		'/News/Alternative/',
		'/News/Breaking_News/',
		'/News/Current_Events/',
		'/News/Extended_Coverage/'
	]
	start_urls = ['http://www.dmoz.org' + path for path in crawl_paths]
	
	def __follow_url(self, url):
		for path in self.crawl_paths:
			if path in url:
				return True
		return False

	def parse(self, response):		
		parser = HtmlXPathSelector(response)
		urls = parser.select('//a/@href').extract()
		for url in urls:
			if url.startswith('http://www.dmoz.org'):
				if self.__follow_url(url):
					yield Request(url, callback=self.parse)
			elif url.startswith('/'):
				if self.__follow_url(url):
					yield Request('http://www.dmoz.org' + url, callback=self.parse)
			elif url.startswith('http://'):
				item = LinkItem()
				item['url'] = url
				yield item
