# coding: utf-8

from scrapy import Spider
from scrapy import FormRequest
from scrapy import Request
from scrapy.selector import Selector
from time import strftime, strptime, mktime, localtime, time
import re
import json
import uuid

from byrbbs.items import postItem, commentItem
from byrbbs.mysqlclient import get_mysql
from byrbbs.SpiderConfig import SpiderConfig


class UpdateSpider(Spider):
    name = "updatespider"
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

    # 遍历所有的版块
    def logged_in(self, response):
        login_info = json.loads(response.body.decode('gbk'))

        if login_info['ajax_msg'] != u'操作成功':
            print 'ERROR!!!'
            return
        mh = get_mysql()
        # 清除user_id中的数据
        sql = "delete from user_id"
        mh.execute(sql)

        # 获取post中最大id到数据库中
        sql = "select id from post order by id desc limit 1"
        ret = mh.select(sql)

        sql = "update post_last_id set post_id='%s' where id=1" % ret[0][0]
        mh.execute(sql)

        # 删除post_delete
        delete_sql = "delete from post_delete"
        mh.execute(delete_sql)

        # 从数据库中找出每个版块的名称
        sql = "select board_name from board"
        ret_sql = mh.select(sql)

        for ret in ret_sql:
            board = ret[0]
            section_url = 'https://bbs.byr.cn/board/%s' % board
            yield Request(section_url,
                          meta={'board_name': board, 'cookiejar': response.meta['cookiejar']},
                          headers={'X-Requested-With': 'XMLHttpRequest'},
                          callback=self.board_page)

    # 爬取每个版块的帖子的页数
    def board_page(self, response):
        # sel = Selector(response)
        sql = "SELECT `last_time` FROM post WHERE `board_name`='%s' " \
              "ORDER BY `last_time` DESC LIMIT 1" % response.meta['board_name']
        mh = get_mysql()
        ret_sql = mh.select(sql)

        if not ret_sql:
            cmp_time = 0
        else:
            last_time = str(ret_sql[0][0])
            time_tmp = strptime(last_time, "%Y-%m-%d %H:%M:%S")
            cmp_time = int(mktime(time_tmp))

        # 更新时间范围为一周的帖子，604800秒等于一周, 172800秒等于2天
        now_time = int(time())
        if cmp_time + 172800 < now_time:
            cmp_time = now_time

        # PostNum_xpath = '/html/body/div[4]/div[1]/ul/li[1]/i/text()'
        # post_num = int(sel.xpath(PostNum_xpath).extract()[0])
        #
        # if post_num % 30 == 0:
        #     post_page = post_num / 30 + 1
        # else:
        #     post_page = post_num / 30 + 2

        # 增量更新需要爬取的页数
        for num in xrange(1, 3):  # post_page):
            page_url = 'https://bbs.byr.cn/board/%s?p=%s' % (response.meta['board_name'], num)

            yield Request(page_url,
                          meta={'cmp_time': cmp_time,
                                'page': num,
                                'board_name': response.meta['board_name'],
                                'cookiejar': response.meta['cookiejar']},
                          headers={'X-Requested-With': 'XMLHttpRequest'},
                          callback=self.post_list)

    # 增量更新爬取的帖子
    def post_list(self, response):
        sel = Selector(response)

        for i in xrange(1, 31):
            PostURL_xpath = '/html/body/div[3]/table/tbody/tr[%s]/td[2]/a/@href' % i
            PostTitle_xpath = '/html/body/div[3]/table/tbody/tr[%s]/td[2]/a/text()' % i
            AuthorID_xpath = '/html/body/div[3]/table/tbody/tr[%s]/td[4]/a/text()' % i
            PostNum_xpath = '/html/body/div[3]/table/tbody/tr[%s]/td[5]/a/text()' % i
            LastTime_xpath = '/html/body/div[3]/table/tbody/tr[%s]/td[6]/a/text()' % i

            try:
                post_title = sel.xpath(PostTitle_xpath).extract()[0]
                post_url = sel.xpath(PostURL_xpath).extract()[0]
                author_id = sel.xpath(AuthorID_xpath).extract()[0]
                post_num = int(sel.xpath(PostNum_xpath).extract()[0])
                last_time = sel.xpath(LastTime_xpath).extract()[0]
            except:
                return

            try:
                time_tmp = strptime(last_time, "%Y-%m-%d")
                cmp_time2 = int(mktime(time_tmp)) + 86400
            except:
                last_time = strftime('%Y-%m-%d', localtime(time())) + ' ' + last_time.strip().encode('utf-8')
                time_tmp = strptime(last_time, "%Y-%m-%d %H:%M:%S")
                cmp_time2 = int(mktime(time_tmp))

            if cmp_time2 <= response.meta['cmp_time'] and response.meta['page'] == 1:
                continue
            elif cmp_time2 <= response.meta['cmp_time'] and response.meta['page'] > 1:
                break

            post_url = 'https://bbs.byr.cn' + post_url

            mh = get_mysql()
            # 查找回帖数是否改变
            search_sql = "select post_num from post where url='%s'" % post_url
            ret = mh.execute(search_sql)
            if ret and ret[0][0] == post_num:
                continue

            print post_url
            # 插入新的要删除的post
            insert_sql = "insert into post_delete select id, title, content from post where url='%s'" % post_url
            mh.execute(insert_sql)

            # 删除更新的原贴和评论
            sql = "DELETE FROM comment WHERE `post_id`=(SELECT `post_id` FROM post WHERE `url`='%s')" % post_url
            mh.execute(sql)

            sql = "DELETE FROM post WHERE `url`='%s'" % post_url
            mh.execute(sql)

            yield Request(post_url,
                          meta={'cookiejar': response.meta['cookiejar'],
                                'post_title': post_title,
                                'post_url': post_url,
                                'author_id': author_id,
                                'last_time': last_time,
                                'board_name': response.meta['board_name']},
                          headers={'X-Requested-With': 'XMLHttpRequest'},
                          callback=self.post_spider)

    # 爬取帖子的内容
    def post_spider(self, response):
        sel = Selector(response)
        item = postItem()

        # 帖子信息
        item['post_title'] = response.meta['post_title']
        item['post_url'] = response.meta['post_url']
        item['board_name'] = response.meta['board_name']

        if response.meta['author_id'] == u'原帖已删除':
            item['author_id'] = response.meta['author_id']
            item['author_name'] = ''
            item['post_content'] = ''

            comment_snum = 1
        else:
            # 作者id和用户名
            try:
                AuthorInfo_xpath = '/html/body/div[3]/div[1]/table/tr[2]/td[2]/div/text()[1]'
                author_info = sel.xpath(AuthorInfo_xpath).extract()[0]

                item['author_id'] = re.findall(r': (.+?) \(', author_info)[0]

                try:
                    item['author_name'] = re.findall(r'\((.+?)\)', author_info)[0]
                except:
                    item['author_name'] = ''
            except:
                try:
                    AuthorId_xpath = '/html/body/div[3]/div[1]/table/tr[1]/td[1]/span[1]/a/text()'
                    item['author_id'] = sel.xpath(AuthorId_xpath).extract()[0]
                except:
                    try:
                        AuthorId_xpath = '/html/body/div[3]/div[1]/table/tr[1]/td[1]/span[1]/text()'
                        item['author_id'] = sel.xpath(AuthorId_xpath).extract()[0]
                    except:
                        return

                AuthorName_xpath = '/html/body/div[3]/div[1]/table/tr[2]/td[1]/div[2]/text()'
                try:
                    item['author_name'] = sel.xpath(AuthorName_xpath).extract()[0]
                except:
                    item['author_name'] = ''
            # 帖子内容
            try:
                PostContent_xpath = '/html/body/div[3]/div[1]/table/tr[2]/td[2]/div'
                post_content = sel.xpath(PostContent_xpath).extract()[0]
            except:
                try:
                    post_content = re.findall(r'<td class="a-content">(.+?)</td>', response.body.decode('gbk'), re.DOTALL)[0]
                except:
                    try:
                        post_content = re.findall(r'<td class="a-content">(.+?)</td>', response.body, re.DOTALL)[0].decode('gbk')
                    except:
                        post_content = re.findall(r'<td class="a-content">(.+?)</td>', response.body, re.DOTALL)[0]

            if re.findall(r'<br>', post_content, re.DOTALL):
                try:
                    post_content = re.findall(r'.+?<br>.+?<br>.+?<br>(.+)', post_content, re.DOTALL)[0]
                except:
                    pass

            post_content2 = post_content[0:10000]

            if re.findall(r'<font class=', post_content2, re.DOTALL):
                try:
                    post_content = re.findall(r'(.+?)<font class=', post_content2, re.DOTALL)[0]
                except:
                    pass
            else:
                post_content = post_content[0:65535]
                if re.findall(r'<font class=', post_content, re.DOTALL):
                    try:
                        post_content = re.findall(r'(.+?)<font class=', post_content, re.DOTALL)[0]
                    except:
                        pass
                else:
                    try:
                        post_content = re.findall(r'(.+?)</div>', post_content, re.DOTALL)[0]
                    except:
                        pass

            post_content = re.sub(r'<[\w|/].+?>', '', post_content)
            item['post_content'] = post_content.strip('--')

            comment_snum = 2

        # item类型
        item['type'] = 'post_update'

        # 帖子id
        post_id = str(uuid.uuid1()).split('-')
        item['post_id'] = post_id[0] + post_id[1] + post_id[3]

        # 初始最后跟帖时间
        item['last_time'] = response.meta['last_time']

        # 跟帖数量
        try:
            PostNum_xpath = '/html/body/div[1]/div[1]/ul/li[1]/i/text()'
            item['post_num'] = str(int(sel.xpath(PostNum_xpath).extract()[0]) - 1)
        except:
            try:
                PostNum_xpath = '/html/body/div[4]/div[1]/ul/li[1]/i/text()'
                item['post_num'] = str(int(sel.xpath(PostNum_xpath).extract()[0]) - 1)
            except:
                item['post_num'] = '0'
                item['post_time'] = response.meta['last_time']
                # 增加新的user_id
                sql = "insert into user_id(`user_id`) values('%s')" % item['author_id']
                mh = get_mysql()
                mh.execute(sql)
                yield item
                return

        # 帖子发布时间
        try:
            try:
                PostTime_xpath = '/html/body/div[3]/div[1]/table/tr[2]/td[2]/div//text()[3]'
                post_time = sel.xpath(PostTime_xpath).extract()[0]
                post_time = re.findall(r'\(([\xa0\w :]+?:[\xa0\w :]+?)\)', post_time)[0]
            except:
                try:
                    PostTime_xpath = '/html/body/div[3]/div[1]/table/tr[2]/td[2]/div//text()[4]'
                    post_time = sel.xpath(PostTime_xpath).extract()[0]
                    post_time = re.findall(r'\(([\xa0\w :]+?:[\xa0\w :]+?)\)', post_time)[0]
                except:
                    PostTime_xpath = '/html/body/div[3]/div[1]/table/tr[2]/td[2]/div//text()[5]'
                    post_time = sel.xpath(PostTime_xpath).extract()[0]
                    post_time = re.findall(r'\(([\xa0\w :]+?:[\xa0\w :]+?)\)', post_time)[0]

            post_time = post_time.replace(u'\xa0\xa0', ' ')
            post_time = localtime(mktime(strptime(post_time, "%a %b %d %H:%M:%S %Y")))
            item['post_time'] = strftime('%Y-%m-%d %H:%M:%S', post_time)
        except:
            item['post_time'] = response.meta['last_time']

        # 爬取帖子首页的评论
        for num in xrange(comment_snum, 11):
            item_comment = commentItem()

            # 作者id和用户名
            try:
                CommenterInfo_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div/text()[1]' % num

                commenter_info = sel.xpath(CommenterInfo_xpath).extract()[0]
                item_comment['commenter_id'] = re.findall(r': (.+?) \(', commenter_info)[0]

                try:
                    item_comment['commenter_name'] = re.findall(r'\((.+?)\)', commenter_info)[0]
                except:
                    item_comment['commenter_name'] = ''
            except:
                try:
                    CommenterId_xpath = '/html/body/div[3]/div[%s]/table/tr[1]/td[1]/span[1]/a/text()' % num
                    item_comment['commenter_id'] = sel.xpath(CommenterId_xpath).extract()[0]
                except:
                    try:
                        CommenterId_xpath = '/html/body/div[3]/div[%s]/table/tr[1]/td[1]/span[1]/text()' % num
                        item_comment['commenter_id'] = sel.xpath(CommenterId_xpath).extract()[0]
                    except:
                        break

                CommenterName_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[1]/div[2]/text()' % num
                try:
                    item_comment['commenter_name'] = sel.xpath(CommenterName_xpath).extract()[0]
                except:
                    item_comment['commenter_name'] = ''

            # 评论发布时间
            try:
                try:
                    CommentTime_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div/text()[3]' % num
                    comment_time = sel.xpath(CommentTime_xpath).extract()[0]
                    comment_time = re.findall(r'\(([\xa0\w :]+?:[\xa0\w :]+?)\)', comment_time)[0]
                except:
                    try:
                        CommentTime_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div/text()[4]' % num
                        comment_time = sel.xpath(CommentTime_xpath).extract()[0]
                        comment_time = re.findall(r'\(([\xa0\w :]+?:[\xa0\w :]+?)\)', comment_time)[0]
                    except:
                        CommentTime_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div/text()[5]' % num
                        comment_time = sel.xpath(CommentTime_xpath).extract()[0]
                        comment_time = re.findall(r'\(([\xa0\w :]+?:[\xa0\w :]+?)\)', comment_time)[0]

                comment_time = comment_time.replace(u'\xa0\xa0', ' ')
                comment_time = localtime(mktime(strptime(comment_time, "%a %b %d %H:%M:%S %Y")))
                item_comment['comment_time'] = strftime('%Y-%m-%d %H:%M:%S', comment_time)
            except:
                item_comment['comment_time'] = response.meta['last_time']

            # 帖子最新回复时间
            item['last_time'] = item_comment['comment_time']

            # 评论内容
            try:
                CommentContent_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div' % num
                comment_content = sel.xpath(CommentContent_xpath).extract()[0]
            except:
                try:
                    comment_content = re.findall(r'<td class="a-content">(.+?)</td>', response.body.decode('gbk'), re.DOTALL)[num-1]
                except:
                    try:
                        comment_content = re.findall(r'<td class="a-content">(.+?)</td>', response.body, re.DOTALL)[num-1].decode('gbk')
                    except:
                        comment_content = re.findall(r'<td class="a-content">(.+?)</td>', response.body, re.DOTALL)[num-1]

            if re.findall(r'<br>', comment_content, re.DOTALL):
                try:
                    comment_content = re.findall(r'.+?<br>.+?<br>.+?<br>(.+)', comment_content, re.DOTALL)[0]
                except:
                    pass

            comment_content2 = comment_content[0:10000]

            if re.findall(r'<font class=', comment_content2, re.DOTALL):
                try:
                    comment_content = re.findall(r'(.+?)<font class=', comment_content2, re.DOTALL)[0]
                except:
                    pass
            else:
                comment_content = comment_content[0:65535]
                if re.findall(r'<font class=', comment_content, re.DOTALL):
                    try:
                        comment_content = re.findall(r'(.+?)<font class=', comment_content, re.DOTALL)[0]
                    except:
                        pass
                else:
                    try:
                        comment_content = re.findall(r'(.+?)</div>', comment_content, re.DOTALL)[0]
                    except:
                        pass

            comment_content = re.sub(r'<[\w|/].+?>', '', comment_content)
            item_comment['comment_content'] = comment_content.strip('--')

            # item类型
            item_comment['type'] = 'comment_update'

            # 帖子id
            item_comment['post_id'] = item['post_id']

            # 评论url
            item_comment['comment_url'] = item['post_url']

            # 帖子题目
            item_comment['post_title'] = item['post_title']

            # 增加新的user_id
            sql = "insert into user_id(`user_id`) values('%s')" % item_comment['commenter_id']
            mh = get_mysql()
            mh.execute(sql)
            yield item_comment

        # 判断一共有多少页评论
        post_num = int(item['post_num']) + 1
        if post_num == 1:
            item['last_time'] = item['post_time']
            # 增加新的user_id
            sql = "insert into user_id(`user_id`) values('%s')" % item['author_id']
            mh = get_mysql()
            mh.execute(sql)
            yield item
            return
        elif post_num <= 10:
            # 增加新的user_id
            sql = "insert into user_id(`user_id`) values('%s')" % item['author_id']
            mh = get_mysql()
            mh.execute(sql)
            yield item
            return
        elif post_num % 10 == 0:
            post_page = post_num / 10 + 1
        else:
            post_page = post_num / 10 + 2

        # 爬取帖子首页后面的评论
        for num in xrange(2, post_page):
            page_url = '%s?p=%s' % (item['post_url'], num)
            yield Request(page_url,
                          meta={'cookiejar': response.meta['cookiejar'],
                                'comment_url': page_url,
                                'post_id': item['post_id'],
                                'post_title': item['post_title'],
                                'board_name': response.meta['board_name'],
                                'last_time': response.meta['last_time'],
                                'post_page': post_page-1,
                                'now_page': num,
                                'item': item},
                          headers={'X-Requested-With': 'XMLHttpRequest'},
                          callback=self.comment_spider)

    # 爬取评论内容
    def comment_spider(self, response):
        sel = Selector(response)
        post_item = response.meta['item']

        for num in xrange(1, 11):
            item = commentItem()

            # 作者id和用户名
            try:
                CommenterInfo_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div/text()[1]' % num

                commenter_info = sel.xpath(CommenterInfo_xpath).extract()[0]

                item['commenter_id'] = re.findall(r': (.+?) \(', commenter_info)[0]

                try:
                    item['commenter_name'] = re.findall(r'\((.+?)\)', commenter_info)[0]
                except:
                    item['commenter_name'] = ''
            except:
                try:
                    CommenterId_xpath = '/html/body/div[3]/div[%s]/table/tr[1]/td[1]/span[1]/a/text()' % num
                    item['commenter_id'] = sel.xpath(CommenterId_xpath).extract()[0]
                except:
                    try:
                        CommenterId_xpath = '/html/body/div[3]/div[%s]/table/tr[1]/td[1]/span[1]/text()' % num
                        item['commenter_id'] = sel.xpath(CommenterId_xpath).extract()[0]
                    except:
                        if response.meta['post_page'] == response.meta['now_page']:
                            # 增加新的user_id
                            sql = "insert into user_id(`user_id`) values('%s')" % post_item['author_id']
                            mh = get_mysql()
                            mh.execute(sql)
                            yield post_item
                        return

                CommenterName_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[1]/div[2]/text()' % num
                try:
                    item['commenter_name'] = sel.xpath(CommenterName_xpath).extract()[0]
                except:
                    item['commenter_name'] = ''

            # 评论发布时间
            try:
                try:
                    CommentTime_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div/text()[3]' % num
                    comment_time = sel.xpath(CommentTime_xpath).extract()[0]
                    comment_time = re.findall(r'\(([\xa0\w :]+?:[\xa0\w :]+?)\)', comment_time)[0]
                except:
                    try:
                        CommentTime_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div/text()[4]' % num
                        comment_time = sel.xpath(CommentTime_xpath).extract()[0]
                        comment_time = re.findall(r'\(([\xa0\w :]+?:[\xa0\w :]+?)\)', comment_time)[0]
                    except:
                        CommentTime_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div/text()[5]' % num
                        comment_time = sel.xpath(CommentTime_xpath).extract()[0]
                        comment_time = re.findall(r'\(([\xa0\w :]+?:[\xa0\w :]+?)\)', comment_time)[0]

                comment_time = comment_time.replace(u'\xa0\xa0', ' ')
                comment_time = localtime(mktime(strptime(comment_time, "%a %b %d %H:%M:%S %Y")))
                item['comment_time'] = strftime('%Y-%m-%d %H:%M:%S', comment_time)
            except:
                item['comment_time'] = response.meta['last_time']

            # 评论内容
            try:
                CommentContent_xpath = '/html/body/div[3]/div[%s]/table/tr[2]/td[2]/div' % num
                comment_content = sel.xpath(CommentContent_xpath).extract()[0]
            except:
                try:
                    comment_content = re.findall(r'<td class="a-content">(.+?)</td>', response.body.decode('gbk'), re.DOTALL)[num-1]
                except:
                    try:
                        comment_content = re.findall(r'<td class="a-content">(.+?)</td>', response.body, re.DOTALL)[num-1].decode('gbk')
                    except:
                        comment_content = re.findall(r'<td class="a-content">(.+?)</td>', response.body, re.DOTALL)[num-1]

            if re.findall(r'<br>', comment_content, re.DOTALL):
                try:
                    comment_content = re.findall(r'.+?<br>.+?<br>.+?<br>(.+)', comment_content, re.DOTALL)[0]
                except:
                    pass

            comment_content2 = comment_content[0:10000]

            if re.findall(r'<font class=', comment_content2, re.DOTALL):
                try:
                    comment_content = re.findall(r'(.+?)<font class=', comment_content2, re.DOTALL)[0]
                except:
                    pass
            else:
                comment_content = comment_content[0:65535]
                if re.findall(r'<font class=', comment_content, re.DOTALL):
                    try:
                        comment_content = re.findall(r'(.+?)<font class=', comment_content, re.DOTALL)[0]
                    except:
                        pass
                else:
                    try:
                        comment_content = re.findall(r'(.+?)</div>', comment_content, re.DOTALL)[0]
                    except:
                        pass

            comment_content = re.sub(r'<[\w|/].+?>', '', comment_content)
            item['comment_content'] = comment_content.strip('--')

            # 帖子最新回复时间
            if response.meta['post_page'] == response.meta['now_page']:
                post_item['last_time'] = item['comment_time']

            # item类型
            item['type'] = 'comment_update'

            # 帖子id
            item['post_id'] = response.meta['post_id']

            # 评论url
            item['comment_url'] = response.meta['comment_url']

            # 帖子题目
            item['post_title'] = response.meta['post_title']

            # 增加新的user_id
            sql = "insert into user_id(`user_id`) values('%s')" % item['commenter_id']
            mh = get_mysql()
            mh.execute(sql)
            yield item

        if response.meta['post_page'] == response.meta['now_page']:
            # 增加新的user_id
            sql = "insert into user_id(`user_id`) values('%s')" % post_item['author_id']
            mh = get_mysql()
            mh.execute(sql)
            yield post_item
