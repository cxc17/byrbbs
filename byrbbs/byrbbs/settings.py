# coding: utf-8
# Scrapy settings for byrbbs project
from SpiderConfig import SpiderConfig

LOG_LEVEL = 'INFO'

BOT_NAME = 'byrbbs'

SPIDER_MODULES = ['byrbbs.spiders']
NEWSPIDER_MODULE = 'byrbbs.spiders'

ITEM_PIPELINES = {'byrbbs.pipelines.ByrbbsPipeline': 100}
MYEXT_ENABLED = 1
EXTENSIONS = {'byrbbs.extensions.SpiderOpenCloseLogging': 500}

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'byrbbs (+http://www.yourdomain.com)'
USER_AGENT = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"

if SpiderConfig.spider_type == 'userspider' or SpiderConfig.spider_type == 'userupdate':
    DOWNLOAD_DELAY = 1

CONCURRENT_REQUESTS = 32
