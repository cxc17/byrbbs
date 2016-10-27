# coding: utf-8

from byrbbs.SpiderConfig import SpiderConfig
from byrbbs.mysqlclient import get_mysql

import re
from lxml import etree
import requests


class BBSsearch(object):
    def __init__(self):
        pass

    # 以发帖人的ID或标题中的词语为关键字查找帖子
    def keyword_search(self, session):
        for j in xrange(499, 501):
            url = "https://bbs.byr.cn/article/Constellations/462125?p=%s" % j

            req = session.get(url, headers={'X-Requested-With': 'XMLHttpRequest'})
            try:
                html = req.content.decode("gbk")
            except:
                html = req.content
            sel = etree.HTML(html)

            for i in xrange(1, 11):
                xpath_num = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div/text()' % i
                xpath_user = "/html/body/div[3]/div[%s]/table/tr[1]/td[1]/span[1]/a/text()" % i
                xpath_user2 = "/html/body/div[3]/div[%s]/table/tr[1]/td[1]/span[1]/text()" % i 
                xpath_gender = "/html/body/div[3]/div[%s]/table/tr[1]/td[1]/span[2]/samp/@class" % i
                
                try:
                    content =  ''.join(sel.xpath(xpath_num)[3:])
                    gender = sel.xpath(xpath_gender)[0]
                except Exception, e:
                    print url, i, '123123'

                try:
                    user_id = sel.xpath(xpath_user)[0]
                except Exception, e:
                    try:
                        user_id = sel.xpath(xpath_user2)[0]
                    except Exception, e:
                        print url, i
                    

                if u'woman' in gender:
                    gender = u"女生"
                elif u'man' in gender:
                    gender = u'男生'
                else:
                    gender = u'保密'

                sql = "insert into num_bbs (user_id, content, gender, num, url) values ('%s', '%s', '%s', '0', '%s')" % (user_id, content, gender, url)
                mh = get_mysql()
                try:
                    mh.execute(sql)
                except Exception, e:
                    print content, user_id, gender, url
    
    def login(self, search_type, search_name):
        session = requests.session()

        login_url = 'https://bbs.byr.cn/user/ajax_login.json'
        session.post(login_url, data={'id': SpiderConfig.account_id, 'passwd': SpiderConfig.account_passwd},
                     headers={'X-Requested-With': 'XMLHttpRequest'})

        self.keyword_search(session)
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



def deal_num():
    SpiderConfig.initialize()

    # sql = "select * from num_bbs where num='0'"
    # mh = get_mysql()
    # ret_sql = mh.select(sql)

    sql = "select * from num_bbs where num!='0' order by num"
    mh = get_mysql()
    ret_sql = mh.select(sql)

    from collections import defaultdict

    total = defaultdict(list)
    for ret in ret_sql:
        # if re.findall(r'\d\d\d--', ret[2]):
        #     num = re.findall(r'\d\d\d', ret[2])[0]
        #     sql = "update num_bbs set num='%s' where id='%s' " % (num, ret[0])
        #     mh = get_mysql()
        #     mh.execute(sql)
        
        # if re.findall(u'\d\d\d【 ', ret[2]):
        #     num = re.findall(u'(\d\d\d)【 ', ret[2])[0]
        #     sql = "update num_bbs set num='%s' where id='%s' " % (num, ret[0])
        #     mh = get_mysql()
        #     mh.execute(sql)

        # if re.findall(r'^(\d\d\d)\D', ret[2]):
        #     num = re.findall(r'^(\d\d\d)\D', ret[2])[0]
        #     sql = "update num_bbs set num='%s' where id='%s' " % (num, ret[0])
        #     mh = get_mysql()
        #     mh.execute(sql)

        # if re.findall(r'\D(\d\d\d)\D', ret[2]):
        #     num = re.findall(r'\D(\d\d\d)\D', ret[2])[-1]
        #     sql = "update num_bbs set num='%s' where id='%s' " % (num, ret[0])
        #     mh = get_mysql()
        #     mh.execute(sql)
        if (ret[1], ret[4]) not in total[ret[3]]:
            total[ret[3]].append((ret[1], ret[4]))
        # print ret[1], ret[3], ret[4]

    total = sorted(total.items(), key=lambda i:i[0])
    
    for i in total:
        print i[0], "-----------------"
        for j in i[1]:
            print j[0], j[1]



if __name__ == '__main__':
    # BBSsearch().bbssearch('name', 'oneseven')
    deal_num()





















