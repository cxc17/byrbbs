# coding: utf-8

from byrbbs.SpiderConfig import SpiderConfig
from byrbbs.mysqlclient import get_mysql

from lxml import etree
import requests


class BBSsearch(object):
    count = 1

    def __init__(self):
        pass

    # 以发帖人的ID或标题中的词语为关键字查找帖子
    def keyword_search(self, session, section_url, board_name):
        req = session.get(section_url, headers={'X-Requested-With': 'XMLHttpRequest'})
        try:
            html = req.content.decode("gbk")
        except:
            html = req.content

        sel = etree.HTML(html)

        xpath_num = '//div[3]/div/ul/li[1]/i/text()'
        title_num = int(sel.xpath(xpath_num)[0])

        if title_num % 80 == 0:
            page = title_num / 80 - 1
        else:
            page = title_num / 80

        for board_num in xrange(1, 81):
            try:
                xpath_name = '//div[2]/table/tr[%s]/td[2]/a/text()' % board_num
                xpath_url = '//div[2]/table/tr[%s]/td[2]/a/@href' % board_num
                title = sel.xpath(xpath_name)[0]
                url = 'https://bbs.byr.cn/' + sel.xpath(xpath_url)[0]
            except:
                break

            print self.count, board_name
            print title, url
            self.count += 1

        for i in xrange(page):
            section_url += '&p=%s' % (i+2)
            req = session.get(section_url, headers={'X-Requested-With': 'XMLHttpRequest'})
            try:
                html = req.content.decode("gbk")
            except:
                html = req.content

            sel = etree.HTML(html)

            for board_num in xrange(1, 81):
                try:
                    xpath_name = '//div[2]/table/tr[%s]/td[2]/a/text()' % board_num
                    xpath_url = '//div[2]/table/tr[%s]/td[2]/a/@href' % board_num
                    title = sel.xpath(xpath_name)[0]
                    url = 'https://bbs.byr.cn/' + sel.xpath(xpath_url)[0]
                except:
                    break

                print self.count, board_name
                print title, url
                self.count += 1

    def login(self, search_type, search_name):
        session = requests.session()

        login_url = 'https://bbs.byr.cn/user/ajax_login.json'
        session.post(login_url, data={'id': SpiderConfig.account_id, 'passwd': SpiderConfig.account_passwd},
                     headers={'X-Requested-With': 'XMLHttpRequest'})

        # 从数据库中找出每个版块的名称
        sql = "select board_name from board"
        mh = get_mysql()
        ret_sql = mh.select(sql)

        for row in ret_sql:

            if search_type == 'name':
                # 以发帖人的ID为关键字查找帖子
                section_url = 'https://bbs.byr.cn/s/article?au=%s&b=%s' % (search_name, row[0])
                self.keyword_search(session, section_url, row[0])
            elif search_type == 'title':
                # 以标题中的词语为关键字查找帖子
                section_url = 'https://bbs.byr.cn/s/article?t1=%s&b=%s' % (search_name, row[0])
                self.keyword_search(session, section_url, row[0])
            else:
                print "search_type ERROR!!!"
                return

    def bbssearch(self, search_type=None, search_name=None):
        if not search_type:
            print "search_type None!!!"
            return

        if not search_name:
            print "search_name None!!!"
            return

        SpiderConfig.initialize()
        self.login(search_type, search_name)

if __name__ == '__main__':
    BBSsearch().bbssearch('name', 'oneseven')
