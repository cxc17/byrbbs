# coding: utf-8

from scrapy import Spider
from scrapy import FormRequest
from scrapy import Request
from time import strftime, localtime
import requests
import json
import re

from byrbbs.items import userItem
from byrbbs.mysqlclient import get_mysql
from byrbbs.SpiderConfig import SpiderConfig


class UserUpdateSpider(Spider):
    name = "userupdate"
    allowed_domains = ["bbs.byr.cn"]
    start_urls = (
        'https://bbs.byr.cn/user/ajax_login.json',
    )

    def __init__(self):
        pass

    # 登录byr论坛
    def start_requests(self):
        return [FormRequest('https://bbs.byr.cn/user/ajax_login.json',
                            meta={'cookiejar': 1},
                            formdata={'id': SpiderConfig.account_id, 'passwd': SpiderConfig.account_passwd},
                            callback=self.logged_in,
                            headers={'X-Requested-With': 'XMLHttpRequest'})]

    def logged_in(self, response):
        login_info = json.loads(response.body.decode('gbk'))

        if login_info['ajax_msg'] != u'操作成功':
            print 'ERROR!!!'
            return

        mh = get_mysql()

        # 从数据库中找出user中存在的user_id
        sql = "select distinct user_id from user_id where user_id in (select user_id from user_id_exsit)"
        ret_sql = mh.select(sql)

        for ret in ret_sql:
            if len(ret[0]) > 12:
                continue
            user_id = ret[0]
            self.deal_user_id(user_id)

        # 从数据库中找出user中不存在的user_id并保存
        sql = "select distinct user_id from user_id where user_id not in (select user_id from user_id_exsit)"
        ret_sql = mh.select(sql)

        sql = "insert into user_id_exsit (`user_id`) select distinct user_id from user_id where user_id not in " \
              "(select user_id from user_id_exsit)"
        mh.execute(sql)

        for ret in ret_sql:
            if len(ret[0]) > 12:
                continue
            user_id = ret[0]
            user_url = 'https://bbs.byr.cn/user/query/%s.json' % user_id
            yield Request(user_url,
                          meta={'cookiejar': response.meta['cookiejar']},
                          headers={'X-Requested-With': 'XMLHttpRequest'},
                          callback=self.user_info)

    def user_info(self, response):
        try:
            user_info = json.loads(response.body.decode('gbk'))
        except:
            return

        if user_info['ajax_msg'] != u"操作成功":
            return
        else:
            item = userItem()

        item['type'] = "user"
        item['user_id'] = user_info['id']
        item['user_name'] = user_info['user_name']

        if user_info['gender'] == u'f':
            item['gender'] = u"女生"
        elif user_info['gender'] == u'm':
            item['gender'] = u"男生"
        else:
            item['gender'] = u"保密"

        item['astro'] = user_info['astro']
        item['qq'] = user_info['qq']
        item['msn'] = user_info['msn']

        item['home_page'] = user_info['home_page']
        item['level'] = user_info['level']
        item['post_count'] = user_info['post_count']
        item['score'] = user_info['score']

        item['life'] = user_info['life']
        try:
            item['last_login_time'] = strftime('%Y/%m/%d %H:%M:%S', localtime(float(user_info['last_login_time'])))
        except:
            item['last_login_time'] = ""

        item['last_login_ip'] = user_info['last_login_ip']
        item['last_login_site'], item['country_cn'], item['country_en'], item['province'], item['last_login_bupt'] = self.last_login(item['last_login_ip'])

        item['face_url'] = user_info['face_url']
        item['face_height'] = user_info['face_height']
        item['face_width'] = user_info['face_width']

        item['status'] = user_info['status']
        try:
            item['status'] = item['status'].replace("\n", "")
            item['status'] = item['status'].replace("<span class='blue'>", "")
            item['status'] = item['status'].replace("</span>", "")
        except:
            item['status'] = u"目前不在站上"

        mh = get_mysql()
        # 获取用户post数目
        sql = "select count(*) from post where `user_id`='%s'" % user_info['id']
        ret_sql = mh.select(sql)
        item['post_num'] = ret_sql[0][0]

        # 获取用户comment数目
        sql = "select count(*) from comment where `user_id`='%s'" % user_info['id']
        ret_sql = mh.select(sql)
        item['comment_num'] = ret_sql[0][0]

        # 删除user里原有的user数据
        sql = "DELETE FROM user WHERE `user_id` = '%s'" % user_info['id']
        mh.execute(sql)

        return item

    @staticmethod
    def last_login(last_login_ip):
        # 判断是否是北邮内网ip
        if re.findall(r'^10\.', last_login_ip):
            if re.findall(r'^10\.8\.', last_login_ip):
                last_login_bupt = u"无线网"
            elif re.findall(r'^10\.101\.', last_login_ip):
                last_login_bupt = u"教一"
            elif re.findall(r'^10\.102\.', last_login_ip):
                last_login_bupt = u"教二"
            elif re.findall(r'^10\.103\.', last_login_ip):
                last_login_bupt = u"教三"
            elif re.findall(r'^10\.104\.', last_login_ip):
                last_login_bupt = u"教四"
            elif re.findall(r'^10\.105\.', last_login_ip):
                last_login_bupt = u"主楼"
            elif re.findall(r'^10\.106\.', last_login_ip):
                last_login_bupt = u"教九"
            elif re.findall(r'^10\.107\.', last_login_ip):
                last_login_bupt = u"明光楼"
            elif re.findall(r'^10\.108\.', last_login_ip):
                last_login_bupt = u"新科研楼"
            elif re.findall(r'^10\.109\.', last_login_ip):
                last_login_bupt = u"新科研楼"
            elif re.findall(r'^10\.110\.', last_login_ip):
                last_login_bupt = u"学十创新大本营"
            elif re.findall(r'^10\.201\.', last_login_ip):
                last_login_bupt = u"学一"
            elif re.findall(r'^10\.201\.', last_login_ip):
                last_login_bupt = u"无线网"
            elif re.findall(r'^10\.202\.', last_login_ip):
                last_login_bupt = u"学二"
            elif re.findall(r'^10\.203\.', last_login_ip):
                last_login_bupt = u"学三"
            elif re.findall(r'^10\.204\.', last_login_ip):
                last_login_bupt = u"学四"
            elif re.findall(r'^10\.205\.', last_login_ip):
                last_login_bupt = u"学五"
            elif re.findall(r'^10\.206\.', last_login_ip):
                last_login_bupt = u"学六"
            elif re.findall(r'^10\.208\.', last_login_ip):
                last_login_bupt = u"学八"
            elif re.findall(r'^10\.209\.', last_login_ip):
                last_login_bupt = u"学九"
            elif re.findall(r'^10\.210\.', last_login_ip):
                last_login_bupt = u"学十"
            elif re.findall(r'^10\.211\.', last_login_ip):
                last_login_bupt = u"学十一"
            elif re.findall(r'^10\.213\.', last_login_ip):
                last_login_bupt = u"学十三"
            elif re.findall(r'^10\.215\.', last_login_ip):
                last_login_bupt = u"学二十九"
            else:
                last_login_bupt = u"未知"

            return u"北京市", u'中国', 'China', u'北京', last_login_bupt

        # 从site中找出现有的ip地址
        sql = "select * from site where ip='%s'" % last_login_ip
        mh = get_mysql()
        ret_sql = mh.select(sql)
        # 判断是否是数据库中存在的ip地址
        if ret_sql:
            return ret_sql[0][1], ret_sql[0][2], ret_sql[0][3], ret_sql[0][4], ""

        # 使用taobao的ip接口完成获取地址
        last_login_ip = last_login_ip.replace("*", "0")
        url = "http://ip.taobao.com/service/getIpInfo.php?ip=%s" % last_login_ip

        try:
            req = requests.get(url)
            ip_content = json.loads(req.content)
        except:
            return "", "", "", "", ""

        if ip_content['code'] == -1 or ip_content['code'] == 1:
            return "", "", "", "", ""
        if ip_content['data']['country'] == u'中国':
            return ip_content['data']['region'], "", "", "", ""
        else:
            return ip_content['data']['country'], "", "", "", ""

    def deal_user_id(self, user_id):
        mh = get_mysql()
        # 获取用户post数目
        sql = "select count(*) from post where `user_id`='%s'" % user_id
        ret_sql = mh.select(sql)
        post_num = ret_sql[0][0]

        # 获取用户comment数目
        sql = "select count(*) from comment where `user_id`='%s'" % user_id
        ret_sql = mh.select(sql)
        comment_num = ret_sql[0][0]

        # 更新user中的数据
        sql = "update user set post_num='%s', comment_num='%s' where user_id='%s'" % (post_num, comment_num, user_id)
        mh.execute(sql)
