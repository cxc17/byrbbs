# coding: utf-8

from twisted.enterprise import adbapi
import MySQLdb.cursors
import ConfigParser
import os


class ByrbbsPipeline(object):
    pre_path = os.getcwd().replace(u"\\", u"/")
    # 配置文件路径
    pre_path = pre_path.replace('/byrbbs', '')
    config_path = pre_path + '/byrbbs/byrbbs/spider.conf'

    config = ConfigParser.ConfigParser()
    config.read(config_path)

    host = config.get('database', 'host')
    user = config.get('database', 'user')
    passwd = config.get('database', 'passwd')
    db = config.get('database', 'db')

    def __init__(self):
        self.dbpool = adbapi.ConnectionPool(
            dbapiName='MySQLdb',
            host=self.host,
            db=self.db,
            user=self.user,
            passwd=self.passwd,
            cursorclass=MySQLdb.cursors.DictCursor,
            charset='utf8',
            use_unicode=False
        )

    def process_item(self, item, spider):
        if item['type'] == 'post':
            self.dbpool.runInteraction(self.post_insert, item)
        elif item['type'] == 'comment':
            self.dbpool.runInteraction(self.comment_insert, item)
        elif item['type'] == 'post_update':
            self.dbpool.runInteraction(self.post_update_insert, item)
        elif item['type'] == 'comment_update':
            self.dbpool.runInteraction(self.comment_update_insert, item)
        elif item['type'] == 'user':
            self.dbpool.runInteraction(self.user_insert, item)

        return item
    
    def post_insert(self, tx, item):
        sql = 'INSERT INTO `post` (`post_id`, `title`, `url`, `content`, `user_id`, `user_name`, `board_name`, ' \
              '`post_num`, `publish_time`, `last_time`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        tx.execute(sql, (item['post_id'], item['post_title'], item['post_url'], item['post_content'],
                         item['author_id'], item['author_name'], item['board_name'], item['post_num'],
                         item['post_time'], item['last_time']))

    def comment_insert(self, tx, item):
        sql = 'INSERT INTO `comment` (`post_id`, `title`, `url`, `content`, `user_id`, `user_name`, `publish_time`) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s)'

        tx.execute(sql, (item['post_id'], item['post_title'], item['comment_url'], item['comment_content'],
                         item['commenter_id'], item['commenter_name'], item['comment_time']))

    def post_update_insert(self, tx, item):
        sql = 'INSERT INTO `post` (`post_id`, `title`, `url`, `content`, `user_id`, `user_name`, `board_name`, ' \
              '`post_num`, `publish_time`, `last_time`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        tx.execute(sql, (item['post_id'], item['post_title'], item['post_url'], item['post_content'],
                         item['author_id'], item['author_name'], item['board_name'], item['post_num'],
                         item['post_time'], item['last_time']))

        sql = 'INSERT INTO `user_id` (`user_id`) VALUES (%s)'
        tx.execute(sql, (item['author_id'], ))

    def comment_update_insert(self, tx, item):
        sql = 'INSERT INTO `comment` (`post_id`, `title`, `url`, `content`, `user_id`, `user_name`, `publish_time`) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s)'

        tx.execute(sql, (item['post_id'], item['post_title'], item['comment_url'], item['comment_content'],
                         item['commenter_id'], item['commenter_name'], item['comment_time']))

        sql = 'INSERT INTO `user_id` (`user_id`) VALUES (%s)'
        tx.execute(sql, (item['commenter_id'], ))

    def user_insert(self, tx, item):
        sql = 'INSERT INTO `user` (`user_id`, `post_num`, `comment_num`,`user_name`, `gender`, `astro`, `qq`, `msn`, ' \
              '`home_page`, `level`, `post_count`, `score`, `life`, `last_login_time`, `last_login_ip`, ' \
              '`last_login_site`, `country_cn`, `country_en`, `province`, `last_login_bupt`, `status`, ' \
              '`face_url`, `face_height`, `face_width`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
              '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        tx.execute(sql, (item['user_id'], item['post_num'], item['comment_num'], item['user_name'], item['gender'],
                         item['astro'], item['qq'], item['msn'], item['home_page'], item['level'], item['post_count'],
                         item['score'], item['life'], item['last_login_time'], item['last_login_ip'],
                         item['last_login_site'], item['country_cn'], item['country_en'], item['province'],
                         item['last_login_bupt'], item['status'], item['face_url'], item['face_height'], item['face_width']))
