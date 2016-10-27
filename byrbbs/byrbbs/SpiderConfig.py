# coding: utf-8

import os
import ConfigParser


class SpiderConfig(object):
    # 数据库配置信息
    host = None
    user = None
    passwd = None
    db = None

    # 登录信息配置信息
    account_id = None
    account_passwd = None

    # 爬虫类型
    spider_type = None

    def __init__(self, spider_type):
        SpiderConfig.spider_type = spider_type

    @staticmethod
    def initialize():
        pre_path = os.getcwd().replace(u"\\", u"/")
        # 配置文件路径
        pre_path = pre_path.replace('/byrbbs', '')
        config_path = pre_path + '/byrbbs/byrbbs/byrbbs/spider.conf'

        config = ConfigParser.ConfigParser()
        config.read(config_path)

        # 数据库配置信息
        SpiderConfig.host = config.get('database', 'host')
        SpiderConfig.user = config.get('database', 'user')
        SpiderConfig.passwd = config.get('database', 'passwd')
        SpiderConfig.db = config.get('database', 'db')

        # 登录信息配置信息
        SpiderConfig.account_id = config.get('account_info', 'id')
        SpiderConfig.account_passwd = config.get('account_info', 'passwd')
