# coding: utf-8
# 爬取所有版块的名称

from byrbbs.SpiderConfig import SpiderConfig
from byrbbs.mysqlclient import get_mysql

from lxml import etree
import requests
import re


class WriteBoard(object):
    def __init__(self):
        pass

    def run(self, session, section_url):
        req = session.get(section_url, headers={'X-Requested-With': 'XMLHttpRequest'})
        html = req.content.decode("gbk")
        sel = etree.HTML(html)

        sql = "insert into board (`board_name`) values ('%s')"

        for board_num in xrange(1, 50):
            try:
                xpath_name = '//div[2]/table/tbody/tr[%s]/td[1]/a/@href' % board_num
                board_url = sel.xpath(xpath_name)[0]
            except:
                break
            if 'board' in board_url:
                board_name = re.findall(r'board/(.+)', board_url)[0]
                mh = get_mysql()
                mh.execute(sql % board_name)
                print board_num, board_url
            else:
                section_url = 'https://bbs.byr.cn' + board_url
                print board_num
                self.run(session, section_url)

    def login(self):
        session = requests.session()

        login_url = 'https://bbs.byr.cn/user/ajax_login.json'
        session.post(login_url, data={'id': SpiderConfig.account_id, 'passwd': SpiderConfig.account_passwd},
                     headers={'X-Requested-With': 'XMLHttpRequest'})

        for section_num in xrange(0, 10):
            section_url = 'https://bbs.byr.cn/section/%s' % section_num
            self.run(session, section_url)

    def writeboard(self):
        SpiderConfig.initialize()
        self.login()


if __name__ == '__main__':
    WriteBoard().writeboard()
