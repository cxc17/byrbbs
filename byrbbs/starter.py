# coding: utf-8

from twisted.internet import reactor
from scrapy.utils.project import get_project_settings
from scrapy.crawler import Crawler
from scrapy.spider import log
from scrapy import signals
import sys

from byrbbs.spiders.updatespider import UpdateSpider
from byrbbs.spiders.allspider import AllSpider
from byrbbs.spiders.userspider import UserSpider
from byrbbs.spiders.userupdate import UserUpdateSpider
from byrbbs.SpiderConfig import SpiderConfig


if __name__ == '__main__':
    spider_name = sys.argv[1]
    spider = ""

    if spider_name == 'updatespider':
        spider = UpdateSpider()
        SpiderConfig('updatespider').initialize()
    elif spider_name == 'allspider':
        spider = AllSpider()
        SpiderConfig('allspider').initialize()
    elif spider_name == 'userspider':
        spider = UserSpider()
        SpiderConfig('userspider').initialize()
    elif spider_name == 'userupdate':
        spider = UserUpdateSpider()
        SpiderConfig('userupdate').initialize()
    else:
        print '输入参数有误！'
        exit(1)

    settings = get_project_settings()
    crawler = Crawler(settings)
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.configure()
    crawler.crawl(spider)
    crawler.start()

    # 启动log.
    log.scrapy_info(settings)
    log.start_from_settings(settings, crawler)

    reactor.run()
