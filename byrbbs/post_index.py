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

        # 停用词文档频率获取
        sql = "SELECT word, status from stop_word"
        stop_ret = mh2.select(sql)
        stop_words = {}
        for stop_word in stop_ret:
            stop_words[stop_word[0]] = stop_word[1]

        for i in xrange(100000):
            ret = mh.selectone()
            # 对标题进行词频分析
            content = collections.defaultdict(int)
            title_tmp = re.sub(seg, " ".decode("utf8"), ret[1])
            title_tmp = list(jieba.cut(title_tmp))
            for word in title_tmp:
                # 标题词频：出现1次记为10次
                content[word] += 10
            # 对内容进行词频分析
            content_tmp = re.sub(seg, " ".decode("utf8"), ret[2])
            content_tmp = list(jieba.cut(content_tmp))
            for word in content_tmp:
                content[word] += 1

            # 查询post_index中已有的词语
            sql = "SELECT word, doc_fre, list from post_index WHERE "
            for word in content:
                sql += "word='%s' or " % word
            sql = sql.strip("or ")
            word_rets = mh2.select(sql)
            words = {}
            for word_ret in word_rets:
                words[word_ret[0]] = (word_ret[1], word_ret[2])

            update_index = []
            insert_index = []
            # 进行整理倒排索引，更新或插入数据库
            for word, val in content.items():
                if word == u' ':
                    continue

                if word in words.keys():
                    if word in stop_words.keys():
                        doc_fre = words[word][0] + stop_words[word]
                    else:
                        doc_fre = words[word][0] + 1
                    index_list = json.loads(words[word][1])
                    index_list[ret[0]] = val
                    index_list = json.dumps(index_list)
                    update_index.append([word, doc_fre, index_list])
                else:
                    if word in stop_words.keys():
                        doc_fre = stop_words[word]
                    else:
                        doc_fre = 1
                    index_list = {ret[0]: val}
                    index_list = json.dumps(index_list)
                    insert_index.append([word, doc_fre, index_list])
            # 插入操作
            if insert_index:
                sql = "insert into post_index(`word`, doc_fre, list) values "
                for insert in insert_index:
                    sql += "('%s', '%s', '%s')," % (insert[0], insert[1], insert[2])
                sql = sql.strip(",")
                mh2.execute(sql)
            # 更新操作
            if update_index:
                doc_fres = ""
                lists = ""
                ids = ""
                for update in update_index:
                    doc_fres += "when '%s' then '%s' " % (update[0], update[1])
                    lists += "when '%s' then '%s' " % (update[0], update[2])
                    ids += "'%s'," % update[0]
                ids = "(" + ids.strip(",") + ")"
                sql = "update post_index set doc_fre = case word " + doc_fres + " end, list = case word " + lists + " end where word in " + ids
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
