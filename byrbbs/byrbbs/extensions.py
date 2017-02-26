# coding: utf-8

from scrapy import signals
from scrapy.exceptions import NotConfigured
from lxml import etree
import re
import requests

from SpiderConfig import SpiderConfig
from byrbbs.mysqlclient import get_mysql
from post_index import post_index


class SpiderOpenCloseLogging(object):

    def __init__(self):
        pass

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('MYEXT_ENABLED'):
            raise NotConfigured
        ext = cls()
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)

        return ext

    def spider_opened(self, spider):
        print "*"*50 + "Spider Opened Extension" + "*"*50

    def spider_closed(self, spider):
        print "*"*50 + "Spider Closed Extension" + "*"*50
        if SpiderConfig.spider_type == 'userspider' or SpiderConfig.spider_type == 'userupdate':
            SpiderOpenCloseLogging.deal_login_site()
        elif SpiderConfig.spider_type == 'updatespider':
            SpiderOpenCloseLogging.update_index()

    # 更新索引信息
    @staticmethod
    def update_index():
        post_index.index_update()

    # 整理登录ip地址
    @staticmethod
    def deal_login_site():
        # 使用http://ip.zxinc.org/查找剩余IP
        mh = get_mysql()
        sql = "select id, last_login_ip from user where (`last_login_site`='' or `last_login_site`='未分配或者内网IP')" \
              " and last_login_ip!=''"
        ret_info = mh.select(sql)

        for ret in ret_info:
            last_login_ip = ret[1].replace("*", "0")
            url = "http://ip.zxinc.org/ipquery/?ip=%s" % last_login_ip
            req = requests.get(url)
            html = etree.HTML(req.content)
            last_login_site = html.xpath("/html/body/center/div/form[1]/table/tr[4]/td[2]/text()")[0].split(' ')[0]

            if last_login_site == u'局域网' or last_login_site == u'IANA特殊地址' or last_login_site == u'中国' or u'北京'in last_login_site:
                last_login_site = u'北京市'
            elif u'省'in last_login_site:
                last_login_site = re.findall(ur'(.+?省)', last_login_site)[0]
            elif u'美国'in last_login_site:
                last_login_site = u'美国'
            elif u'北美'in last_login_site:
                last_login_site = u'美国'
            elif u'天津'in last_login_site:
                last_login_site = u'天津市'
            elif u'上海'in last_login_site:
                last_login_site = u'上海市'
            elif u'新疆'in last_login_site:
                last_login_site = u'新疆维吾尔自治区'
            elif u'广西'in last_login_site:
                last_login_site = u'广西壮族自治区'
            elif u'宁夏'in last_login_site:
                last_login_site = u'宁夏回族自治区'
            elif u'内蒙古'in last_login_site:
                last_login_site = u'内蒙古自治区'
            elif u'重庆'in last_login_site:
                last_login_site = u'重庆市'
            elif u'西藏'in last_login_site:
                last_login_site = u'西藏自治区'
            sql = "update user set last_login_site='%s' where id='%s'" % (last_login_site, ret[0])
            mh.select(sql)
        # 统计ip到site中
        SpiderOpenCloseLogging.store_site()
        # 整理归类后的user的site
        SpiderOpenCloseLogging.deal_user_login()

    # 统计ip地址到数据库site中
    @staticmethod
    def store_site():
        mh = get_mysql()
        sql = "SELECT last_login_site, last_login_ip from `user` where last_login_site != '' and country_cn = '' " \
              "and last_login_ip not LIKE '10\.%' and last_login_site not LIKE '%市' and last_login_site not " \
              "LIKE '%省' and last_login_site not LIKE '%区' and last_login_site not in ('台湾', '香港', '澳门') " \
              "GROUP BY last_login_site, last_login_ip"
        ret_info = mh.select(sql)

        country_map = {u'克罗地亚': 'Croatia', u'斯洛伐克': 'Slovakia', u'约旦': 'Jordan', u'波多黎各': 'Puerto Rico', u'冰岛': 'Iceland', u'黑山': 'Montenegro', u'尼泊尔': 'Nepal', u'爱沙尼亚': 'Estonia', u'柬埔寨': 'Cambodia', u'卢森堡': 'Luxembourg', u'伊朗': 'Iran', u"阿曼": "Oman", u"韩国": "South Korea", u"斯洛文尼亚": "Slovenia", u"埃及": "Egypt", u"埃塞俄比亚": "Ethiopia", u"科威特": "Kuwait", u"马来西亚": "Malaysia", u"秘鲁": "Peru", u"伊拉克": "Iraq", u"以色列": "Israel", u"匈牙利": "Hungary", u"苏丹": "Sudan", u"南非": "South Africa", u"德国": "Germany", u"葡萄牙": "Portugal", u"格鲁吉亚": "Georgia", u"利比里亚": "Liberia", u"巴西": "Brazil", u"哥伦比亚": "Colombia", u"澳大利亚": "Australia", u"老挝": "Laos", u"西班牙": "Spain", u"白俄罗斯": "Belarus", u"阿尔巴尼亚": "Albania", u"卡塔尔": "Qatar", u"厄瓜多尔": "Ecuador", u"泰国": "Thailand", u"阿拉伯联合酋长国": "United Arab Emirates", u"墨西哥": "Mexico", u"多米尼加共和国": "Dominican Republic", u"巴基斯坦": "Pakistan", u"塞拉利昂": "Sierra Leone", u"朝鲜": "North Korea", u"阿根廷": "Argentina", u"加拿大": "Canada", u"沙特阿拉伯": "Saudi Arabia", u"俄罗斯": "Russia", u"乌克兰": "Ukraine", u"法国": "France", u"日本": "Japan", u"危地马拉": "Guatemala", u"坦桑尼亚": "United Republic of Tanzania", u"比利时": "Belgium", u"越南": "Vietnam", u"丹麦": "Denmark", u"尼日利亚": "Nigeria", u"纳米比亚": "Namibia", u"委内瑞拉": "Venezuela", u"波兰": "Poland", u"美国": "United States of America", u"乌兹别克斯坦": "Uzbekistan", u"荷兰": "Netherlands", u"哈萨克斯坦": "Kazakhstan", u"孟加拉国": "Bangladesh", u"立陶宛": "Lithuania", u"瑞典": "Sweden", u"意大利": "Italy", u"希腊": "Greece", u"爱尔兰": "Ireland", u"斯里兰卡": "Sri Lanka", u"菲律宾": "Philippines", u"英国": "United Kingdom", u"牙买加": "Jamaica", u"印度尼西亚": "Indonesia", u"阿尔及利亚": "Algeria", u"瑞士": "Switzerland", u"印度": "India", u"缅甸": "Myanmar", u"芬兰": "Finland", u"奥地利": "Austria", u"贝宁": "Benin", u"挪威": "Norway", u"新西兰": "New Zealand", u"土耳其": "Turkey"}
        for ret in ret_info:
            try:
                country_en = country_map[ret[0]]
            except:
                print "ERROR! site not exist country:",
                print ret[0].encode("utf-8")
                continue
            sql = "insert into site(`site`, `country_cn`, `country_en`, `province`, `ip`) value " \
                  "('%s', '%s', '%s', '', '%s')" % (ret[0], ret[0], country_en, ret[1])
            mh.execute(sql)

        sql = "SELECT last_login_site, last_login_ip from `user` where last_login_site != '' and country_cn = '' " \
              "and last_login_ip not LIKE '10\.%' and ( last_login_site LIKE '%市' or last_login_site LIKE '%省' or " \
              "last_login_site LIKE '%区' or last_login_site in ('台湾', '香港', '澳门') ) GROUP BY " \
              "last_login_site, last_login_ip"
        ret_info = mh.select(sql)

        for ret in ret_info:
            province = ret[0].strip(u'市').strip(u'省').strip(u'维吾尔自治区').strip(u'壮族自治区').strip(u'回族自治区').strip(u'自治区')
            sql = "insert into site(`site`, `country_cn`, `country_en`, `province`, `ip`) value ('%s', '%s', 'China'," \
                  " '%s', '%s')" % (ret[0], u'中国', province, ret[1])
            mh.execute(sql)

    # 整理归类后的user的site
    @staticmethod
    def deal_user_login():
        mh = get_mysql()
        sql = "select id, last_login_ip from user where `country_cn`='' and last_login_ip!=''"
        ret_info = mh.select(sql)

        for ret in ret_info:
            sql = "select country_cn, country_en, province from site where ip='%s'" % ret[1]
            try:
                site_info = mh.select(sql)[0]
            except:
                print "ERROR! site not exist ip:%s" % ret[1]
                continue

            sql = "update user set `country_cn`='%s', `country_en`='%s', `province`='%s' where id='%s'" % (site_info[0], site_info[1], site_info[2], ret[0])
            mh.select(sql)
