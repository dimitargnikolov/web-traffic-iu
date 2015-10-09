# Scrapy settings for newscrawler project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#
import os

BOT_NAME = 'newscrawler'

ITEM_PIPELINES = [
	'newscrawler.pipelines.ValidNewsSourcePipeline'
]
SPIDER_MODULES = ['newscrawler.spiders']
NEWSPIDER_MODULE = 'newscrawler.spiders'

NEWS_SOURCES_FILE = os.path.join(os.getenv("TD"), "news-urls.txt")
