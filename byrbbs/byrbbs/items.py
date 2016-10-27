# coding: utf-8

import scrapy


class postItem(scrapy.Item):
    type = scrapy.Field()
    post_id = scrapy.Field()
    post_title = scrapy.Field()
    post_url = scrapy.Field()
    post_content = scrapy.Field()
    author_id = scrapy.Field()
    author_name = scrapy.Field()
    board_name = scrapy.Field()
    post_num = scrapy.Field()
    post_time = scrapy.Field()
    last_time = scrapy.Field()


class commentItem(scrapy.Item):
    type = scrapy.Field()
    post_id = scrapy.Field()
    post_title = scrapy.Field()
    comment_url = scrapy.Field()
    comment_content = scrapy.Field()
    commenter_id = scrapy.Field()
    commenter_name = scrapy.Field()
    comment_time = scrapy.Field()


class userItem(scrapy.Item):
    type = scrapy.Field()
    user_id = scrapy.Field()
    post_num = scrapy.Field()
    comment_num = scrapy.Field()
    user_name = scrapy.Field()
    gender = scrapy.Field()
    astro = scrapy.Field()
    qq = scrapy.Field()
    msn = scrapy.Field()
    home_page = scrapy.Field()
    level = scrapy.Field()
    post_count = scrapy.Field()
    score = scrapy.Field()
    life = scrapy.Field()
    last_login_time = scrapy.Field()
    last_login_ip = scrapy.Field()
    last_login_site = scrapy.Field()
    country_cn = scrapy.Field()
    country_en = scrapy.Field()
    province = scrapy.Field()
    last_login_bupt = scrapy.Field()
    status = scrapy.Field()
    face_url = scrapy.Field()
    face_height = scrapy.Field()
    face_width = scrapy.Field()
