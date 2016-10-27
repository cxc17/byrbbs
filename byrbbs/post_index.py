# coding: utf-8
# 建立post索引
from byrbbs.mysqlclient import get_mysql
from byrbbs.SpiderConfig import SpiderConfig

import collections
import jieba
import json
import re


class post_index(object):
    def __init__(self):
        SpiderConfig.initialize()

    @staticmethod
    def word_segmentation():
        mh = get_mysql()
        mh2 = get_mysql()
        seg = ur"[\s+\.\!\/\"\'\-\|\]\[\{\}\\]+|[+——！，。：、~@#￥%……&*（）；－):①②③④⑤⑥⑦⑧⑨⑩⑴⑵⑶⑷⑸⑹⑺─┅┆┈┍┎┒┙├┝┟┣┫┮┰┱┾┿╃╄╆╈╋▉▲▼※Ⅱ←→↖↗↘↙《》┊┋┇┉┠┨┌┐┑└┖┘┚┤┥┦┏┓┗┛┯┷┻┳┃━〓¤`˙⊙│〉〈⒂～？．＂”“’·■／￣�︱〕〔【】』『」「◎○◆●☉　┬┴┸┼═╔╗╚╝╩╭╮╯╰╱╲▁▅▆▇█▊▌▍▎▓▕□▽◇◢◣◤◥★☆︶︻︼︵︿﹃﹎﹏△▔▏▋▄▃▂△▔▏▋▄▃▂╳╬╫╪╨╧╦╥╣╢╠╟╜╙╘╖╓║╅╂┭┕┄〞〝〗〖〒〇〃⿻⿺⿹⿸⿷⿵⿴⿲]+"

        sql = "select id, title, content from post limit 100000"
        mh.query(sql)

        for i in xrange(100000):
            ret = mh.selectone()
            # 对标题进行词频分析
            content = collections.defaultdict(int)
            title_tmp = re.sub(seg, " ".decode("utf8"), ret[1])
            title_tmp = list(jieba.cut(title_tmp))
            for word in title_tmp:
                # 标题词频：出现1次记为10次
                content[word] += 10
            # 对内容进行索引分析
            content_tmp = re.sub(seg, " ".decode("utf8"), ret[2])
            content_tmp = list(jieba.cut(content_tmp))
            for word in content_tmp:
                content[word] += 1

            # 进行整理倒排索引，更新或插入数据库
            for word, val in content.items():
                if word == u' ':
                    continue
                sql = "SELECT post_index.id, post_index.doc_fre, post_index.list, stop_word.status from post_index " \
                      "LEFT JOIN stop_word on post_index.word=stop_word.word WHERE post_index.word='%s'" % word
                word_ret = mh2.select(sql)
                if word_ret:
                    word_ret = word_ret[0]
                    if word_ret[3]:
                        doc_fre = word_ret[1] + word_ret[3]
                    else:
                        doc_fre = word_ret[1] + 1
                    index_list = json.loads(word_ret[2])
                    index_list[ret[0]] = val
                    index_list = json.dumps(index_list)
                    sql = "update post_index set doc_fre='%s', list='%s' where id='%s'" % (doc_fre, index_list, word_ret[0])
                    mh2.execute(sql)
                else:
                    sql = "SELECT status from stop_word WHERE word='%s'" % word
                    stop_ret = mh2.select(sql)
                    if stop_ret:
                        doc_fre = stop_ret[0][0]
                    else:
                        doc_fre = 1
                    index_list = {ret[0]: val}
                    index_list = json.dumps(index_list)
                    sql = "insert into post_index(`word`, doc_fre, list) values ('%s', '%s', '%s') " % (word, doc_fre, index_list)
                    mh2.execute(sql)

    @staticmethod
    def stop_word():
        mh = get_mysql()

        f = open("C:/Users/cxc/Desktop/stop_word.txt")
        content = f.read()
        f.close()
        content = content.split('\n')
        for word in content:
            sql = "select * from stop_word where word='%s'" % word
            ret = mh.select(sql)
            if not ret:
                sql = "insert into stop_word(`word`) values ('%s') " % word
                mh.execute(sql)


if __name__ == '__main__':
    import time
    time1 = time.time()
    post_index().word_segmentation()
    print time.time()-time1
