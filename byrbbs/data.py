# coding: utf-8
# 整理所有数据
from byrbbs.SpiderConfig import SpiderConfig
from byrbbs.mysqlclient import get_mysql

from collections import defaultdict
import json


class DealData(object):
    def __init__(self):
        SpiderConfig.initialize()

    @staticmethod
    # 统计星座信息
    def astro():
        mh = get_mysql()
        sql = "select gender, astro from user"
        ret_info = mh.select(sql)

        astro = {u'女生': defaultdict(int), u'男生': defaultdict(int), u'全部': defaultdict(int)}
        for ret in ret_info:
            if ret[1] == u'' or ret[1] == u'未知':
                continue
            if ret[0] == u'女生':
                astro[u'女生'][ret[1]] += 1
                astro[u'全部'][ret[1]] += 1
            elif ret[0] == u'男生':
                astro[u'男生'][ret[1]] += 1
                astro[u'全部'][ret[1]] += 1
            else:
                continue
        astro = json.dumps(astro, ensure_ascii=False)

        sql = "delete from data where `data_name`='astro'"
        mh.execute(sql)

        sql = "insert into data(`data_name`, `data_value`) values ('astro', '%s')" % astro
        mh.execute(sql)

    @staticmethod
    # 统计北邮用户数
    def user_number_bupt():
        mh = get_mysql()
        sql = "select gender, last_login_bupt from user where last_login_bupt!='' and last_login_bupt!='未知'"
        ret_info = mh.select(sql)

        bupt = defaultdict(int)
        for ret in ret_info:
            bupt[ret[0]] += 1

        bupt = {u'女生': defaultdict(int), u'男生': defaultdict(int), u'全部': defaultdict(int)}
        for ret in ret_info:
            if ret[0] == u'女生':
                bupt[u'女生'][ret[1]] += 1
                bupt[u'全部'][ret[1]] += 1
            elif ret[0] == u'男生':
                bupt[u'男生'][ret[1]] += 1
                bupt[u'全部'][ret[1]] += 1
            else:
                bupt[u'全部'][ret[1]] += 1

        bupt = json.dumps(bupt, ensure_ascii=False)

        sql = "delete from data where `data_name`='bupt'"
        mh.execute(sql)

        sql = "insert into data(`data_name`, `data_value`) values ('bupt', '%s')" % bupt
        mh.execute(sql)

    @staticmethod
    # 统计全国各地区用户数
    def user_number_china():
        mh = get_mysql()
        sql = "select gender, province from user where province!=''"
        ret_info = mh.select(sql)

        china = defaultdict(int)
        for ret in ret_info:
            china[ret[0]] += 1

        china = {u'女生': defaultdict(int), u'男生': defaultdict(int), u'全部': defaultdict(int)}
        for ret in ret_info:
            if ret[0] == u'女生':
                china[u'女生'][ret[1]] += 1
                china[u'全部'][ret[1]] += 1
            elif ret[0] == u'男生':
                china[u'男生'][ret[1]] += 1
                china[u'全部'][ret[1]] += 1
            else:
                china[u'全部'][ret[1]] += 1

        china = json.dumps(china, ensure_ascii=False)

        sql = "delete from data where `data_name`='china'"
        mh.execute(sql)

        sql = "insert into data(`data_name`, `data_value`) values ('china', '%s')" % china
        mh.execute(sql)

    @staticmethod
    # 统计全世界各地区用户数
    def user_number_world():
        mh = get_mysql()
        sql = "select gender, country_en from user where country_en!=''"
        ret_info = mh.select(sql)

        world = defaultdict(int)
        for ret in ret_info:
            world[ret[0]] += 1

        world = {u'女生': defaultdict(int), u'男生': defaultdict(int), u'全部': defaultdict(int)}
        for ret in ret_info:
            if ret[0] == u'女生':
                world[u'女生'][ret[1]] += 1
                world[u'全部'][ret[1]] += 1
            elif ret[0] == u'男生':
                world[u'男生'][ret[1]] += 1
                world[u'全部'][ret[1]] += 1
            else:
                world[u'全部'][ret[1]] += 1

        world = json.dumps(world, ensure_ascii=False)

        sql = "delete from data where `data_name`='world'"
        mh.execute(sql)

        sql = "insert into data(`data_name`, `data_value`) values ('world', '%s')" % world
        mh.execute(sql)

    @staticmethod
    # 统计用户发帖时间分布
    def post_time():
        mh = get_mysql()
        sql = "select post.publish_time, user.gender from post left join user on post.user_id=user.user_id " \
              "ORDER by `publish_time`"
        ret_info = mh.select(sql)

        count = 0
        post_date_num = [0]
        post_date = ["2004/05/28"]
        post_year_num = {u'女生': [0]*13, u'男生': [0]*13, u'全部': [0]*13}
        post_month_num = {u'女生': [0]*12, u'男生': [0]*12, u'全部': [0]*12}
        post_day_num = {u'女生': [0]*31, u'男生': [0]*31, u'全部': [0]*31}
        post_weekday_num = {u'女生': [0]*7, u'男生': [0]*7, u'全部': [0]*7}
        post_hour_num = {u'女生': [0]*24, u'男生': [0]*24, u'全部': [0]*24}
        for ret in ret_info:
            cdatetime = ret[0]
            cdate = str(cdatetime.date()).replace("-", "/")
            cyear = cdatetime.year-2004
            cmonth = cdatetime.month-1
            cday = cdatetime.day-1
            cweekday = cdatetime.weekday()
            chour = cdatetime.hour

            # 日期
            if cdate == post_date[count]:
                post_date_num[count] += 1
            else:
                post_date.append(cdate)
                post_date_num.append(1)
                count += 1

            if ret[1] == u'女生':
                post_year_num[u'女生'][cyear] += 1
                post_year_num[u'全部'][cyear] += 1
                post_month_num[u'女生'][cmonth] += 1
                post_month_num[u'全部'][cmonth] += 1
                post_day_num[u'女生'][cday] += 1
                post_day_num[u'全部'][cday] += 1
                post_weekday_num[u'女生'][cweekday] += 1
                post_weekday_num[u'全部'][cweekday] += 1
                post_hour_num[u'女生'][chour] += 1
                post_hour_num[u'全部'][chour] += 1
            elif ret[1] == u'男生':
                post_year_num[u'男生'][cyear] += 1
                post_year_num[u'全部'][cyear] += 1
                post_month_num[u'男生'][cmonth] += 1
                post_month_num[u'全部'][cmonth] += 1
                post_day_num[u'男生'][cday] += 1
                post_day_num[u'全部'][cday] += 1
                post_weekday_num[u'男生'][cweekday] += 1
                post_weekday_num[u'全部'][cweekday] += 1
                post_hour_num[u'男生'][chour] += 1
                post_hour_num[u'全部'][chour] += 1
            else:
                post_year_num[u'全部'][cyear] += 1
                post_month_num[u'全部'][cmonth] += 1
                post_day_num[u'全部'][chour] += 1
                post_weekday_num[u'全部'][cweekday] += 1
                post_hour_num[u'全部'][chour] += 1

        post = {'post_date': post_date, 'post_date_num': post_date_num, "post_year_num": post_year_num,
                "post_month_num": post_month_num, "post_day_num": post_day_num, "post_weekday_num": post_weekday_num,
                "post_hour_num": post_hour_num}
        post = json.dumps(post, ensure_ascii=False)

        sql = "delete from data where `data_name`='post'"
        mh.execute(sql)

        sql = "insert into data(`data_name`, `data_value`) values ('post', '%s')" % post
        mh.execute(sql)

    @staticmethod
    # 统计用户跟帖时间分布
    def comment_time():
        mh = get_mysql()
        sql = "select comment.publish_time, user.gender from comment left join user on comment.user_id=user.user_id " \
              "ORDER by `publish_time`"
        ret_info = mh.select(sql)

        count = 0
        comment_date_num = [0]
        comment_date = ["2004/05/28"]
        comment_year_num = {u'女生': [0]*13, u'男生': [0]*13, u'全部': [0]*13}
        comment_month_num = {u'女生': [0]*12, u'男生': [0]*12, u'全部': [0]*12}
        comment_day_num = {u'女生': [0]*31, u'男生': [0]*31, u'全部': [0]*31}
        comment_weekday_num = {u'女生': [0]*7, u'男生': [0]*7, u'全部': [0]*7}
        comment_hour_num = {u'女生': [0]*24, u'男生': [0]*24, u'全部': [0]*24}
        for ret in ret_info:
            cdatetime = ret[0]
            cdate = str(cdatetime.date()).replace("-", "/")
            cyear = cdatetime.year-2004
            cmonth = cdatetime.month-1
            cday = cdatetime.day-1
            cweekday = cdatetime.weekday()
            chour = cdatetime.hour

            # 日期
            if cdate == comment_date[count]:
                comment_date_num[count] += 1
            else:
                comment_date.append(cdate)
                comment_date_num.append(1)
                count += 1

            if ret[1] == u'女生':
                comment_year_num[u'女生'][cyear] += 1
                comment_year_num[u'全部'][cyear] += 1
                comment_month_num[u'女生'][cmonth] += 1
                comment_month_num[u'全部'][cmonth] += 1
                comment_day_num[u'女生'][cday] += 1
                comment_day_num[u'全部'][cday] += 1
                comment_weekday_num[u'女生'][cweekday] += 1
                comment_weekday_num[u'全部'][cweekday] += 1
                comment_hour_num[u'女生'][chour] += 1
                comment_hour_num[u'全部'][chour] += 1
            elif ret[1] == u'男生':
                comment_year_num[u'男生'][cyear] += 1
                comment_year_num[u'全部'][cyear] += 1
                comment_month_num[u'男生'][cmonth] += 1
                comment_month_num[u'全部'][cmonth] += 1
                comment_day_num[u'男生'][cday] += 1
                comment_day_num[u'全部'][cday] += 1
                comment_weekday_num[u'男生'][cweekday] += 1
                comment_weekday_num[u'全部'][cweekday] += 1
                comment_hour_num[u'男生'][chour] += 1
                comment_hour_num[u'全部'][chour] += 1
            else:
                comment_year_num[u'全部'][cyear] += 1
                comment_month_num[u'全部'][cmonth] += 1
                comment_day_num[u'全部'][chour] += 1
                comment_weekday_num[u'全部'][cweekday] += 1
                comment_hour_num[u'全部'][chour] += 1

        comment = {'comment_date': comment_date, 'comment_date_num': comment_date_num,
                   "comment_year_num": comment_year_num, "comment_month_num": comment_month_num,
                   "comment_day_num": comment_day_num, "comment_weekday_num": comment_weekday_num,
                   "comment_hour_num": comment_hour_num}
        comment = json.dumps(comment, ensure_ascii=False)

        sql = "delete from data where `data_name`='comment'"
        mh.execute(sql)

        sql = "insert into data(`data_name`, `data_value`) values ('comment', '%s')" % comment
        mh.execute(sql)

if __name__ == '__main__':
    # 统计星座信息
    DealData().astro()

    # 统计北邮用户数
    DealData().user_number_bupt()

    # 统计全国各地区用户数
    DealData().user_number_china()

    # 统计全世界各地区用户数
    DealData().user_number_world()

    # 统计用户发帖时间分布
    DealData().post_time()

    # 统计用户跟帖时间分布
    DealData().comment_time()













