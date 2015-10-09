from scrapy.item import Item, Field

class LinkItem(Item):
	url = Field()

	def __str__(self):
		return self['url']
