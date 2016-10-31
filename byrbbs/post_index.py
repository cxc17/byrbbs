# coding: utf-8
# 建立post索引
from byrbbs.mysqlclient import get_mysql
from byrbbs.SpiderConfig import SpiderConfig

import collections
import jieba
import json
import re
import time
import copy


class post_index(object):
    def __init__(self):
        SpiderConfig.initialize()

    @staticmethod
    def word_segmentation():
        mh = get_mysql()
        mh2 = get_mysql()
        seg = ur"[\s+\.\!\/\"\'\-\|\]\[\{\}\\]+|[+——！，。：、~@#￥%……&*（）；－):①②③④⑤⑥⑦⑧⑨⑩⑴⑵⑶⑷⑸⑹⑺─┅┆┈┍┎┒┙├┝┟┣┫┮┰┱┾┿╃╄╆╈╋▉▲▼※Ⅱ←→↖↗↘↙《》┊┋┇┉┠┨┌┐┑└┖┘┚┤┥┦┏┓┗┛┯┷┻┳┃━〓¤`˙⊙│〉〈⒂～？．＂”“’·■／￣�︱〕〔【】』『」「◎○◆●☉　┬┴┸┼═╔╗╚╝╩╭╮╯╰╱╲▁▅▆▇█▊▌▍▎▓▕□▽◇◢◣◤◥★☆︶︻︼︵︿﹃﹎﹏△▔▏▋▄▃▂△▔▏▋▄▃▂╳╬╫╪╨╧╦╥╣╢╠╟╜╙╘╖╓║╅╂┭┕┄〞〝〗〖〒〇〃⿻⿺⿹⿸⿷⿵⿴⿲]+"

        # 停用词文档频率获取
        sql = "SELECT word, status from stop_word"
        stop_ret = mh2.select(sql)
        stop_words = {}
        for stop_word in stop_ret:
            stop_words[stop_word[0]] = stop_word[1]

        # 获取post数据
        sql = "select id, title, content from post limit 1000"
        mh.query(sql)
        for i in xrange(1000):
            rets = mh.selectmany(1)
            if not rets:
                break
            contents = []
            for num in xrange(len(rets)):
                # 对标题进行词频分析
                contents.append(collections.defaultdict(int))
                title_tmp = re.sub(seg, " ".decode("utf8"), rets[num][1])
                title_tmp = list(jieba.cut(title_tmp))
                for word in title_tmp:
                    # 标题词频：出现1次记为10次
                    contents[num][word] += 10
                # 对内容进行词频分析
                content_tmp = re.sub(seg, " ".decode("utf8"), rets[num][2])
                content_tmp = list(jieba.cut(content_tmp))
                for word in content_tmp:
                    contents[num][word] += 1

            # 查询post_index中已有的词语
            sql = ""
            for content in contents:
                for word in content:
                    sql += "word='%s' or " % word
            sql = "SELECT word, doc_fre, list from post_index WHERE " + sql.strip("or ")
            word_rets = mh2.select(sql)
            words = {}
            for word_ret in word_rets:
                words[word_ret[0]] = (word_ret[1], word_ret[2])

            update_index = {}
            insert_index = {}
            for num in xrange(len(contents)):
                content = contents[num]
                # 进行整理倒排索引，更新或插入数据库
                for word, val in content.items():
                    if word == u' ':
                        continue
                    if word in update_index.keys():
                        if word in stop_words.keys():
                            update_index[word][0] += stop_words[word]
                        else:
                            update_index[word][0] += 1
                        index_list = json.loads(update_index[word][1])
                        index_list[rets[num][0]] = val
                        update_index[word][1] = json.dumps(index_list)
                    elif word in insert_index.keys():
                        if word in stop_words.keys():
                            insert_index[word][0] += stop_words[word]
                        else:
                            insert_index[word][0] += 1
                        index_list = json.loads(insert_index[word][1])
                        index_list[rets[num][0]] = val
                        insert_index[word][1] = json.dumps(index_list)
                    elif word in words.keys():
                        if word in stop_words.keys():
                            doc_fre = words[word][0] + stop_words[word]
                        else:
                            doc_fre = words[word][0] + 1
                        index_list = json.loads(words[word][1])
                        index_list[rets[num][0]] = val
                        index_list = json.dumps(index_list)
                        update_index[word] = [doc_fre, index_list]
                    else:
                        if word in stop_words.keys():
                            doc_fre = stop_words[word]
                        else:
                            doc_fre = 1
                        index_list = {rets[num][0]: val}
                        index_list = json.dumps(index_list)
                        insert_index[word] = [doc_fre, index_list]

            # # 插入操作
            # if insert_index:
            #     sql = "insert into post_index(`word`, doc_fre, list) values "
            #     for k, v in insert_index.items():
            #         sql += "('%s', '%s', '%s')," % (k, v[0], v[1])
            #     sql = sql.strip(",")
            #     mh2.execute(sql)
            # # 更新操作
            # if update_index:
            #     doc_fres = ""
            #     lists = ""
            #     ids = ""
            #     for k, v in update_index.items():
            #         doc_fres += "when '%s' then '%s' " % (k, v[0])
            #         lists += "when '%s' then '%s' " % (k, v[1])
            #         ids += "'%s'," % k
            #     ids = "(" + ids.strip(",") + ")"
            #     sql = "update post_index set doc_fre = case word " + doc_fres + " end, list = case word " + lists + " end where word in " + ids
            #     mh2.execute(sql)

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

    # @staticmethod
    # def create_index():
    #     mh = get_mysql()
    #     seg = ur"[\s+\.\!\/\"\'\-\|\]\[\{\}\\]+|[+——！，。：、~@#￥%……&*（）；－):①②③④⑤⑥⑦⑧⑨⑩⑴⑵⑶⑷⑸⑹⑺─┅┆┈┍┎┒┙├┝┟┣┫┮┰┱┾┿╃╄╆╈╋▉▲▼※Ⅱ←→↖↗↘↙《》┊┋┇┉┠┨┌┐┑└┖┘┚┤┥┦┏┓┗┛┯┷┻┳┃━〓¤`˙⊙│〉〈⒂～？．＂”“’·■／￣�︱〕〔【】』『」「◎○◆●☉　┬┴┸┼═╔╗╚╝╩╭╮╯╰╱╲▁▅▆▇█▊▌▍▎▓▕□▽◇◢◣◤◥★☆︶︻︼︵︿﹃﹎﹏△▔▏▋▄▃▂△▔▏▋▄▃▂╳╬╫╪╨╧╦╥╣╢╠╟╜╙╘╖╓║╅╂┭┕┄〞〝〗〖〒〇〃⿻⿺⿹⿸⿷⿵⿴⿲]+"
    #
    #     # 获取post数据
    #     sql = "select id, title, content from post limit 10"
    #     mh.query(sql)
    #
    #     words = {}
    #     word_num = 0
    #     for i in xrange(10):
    #         insert_index = []
    #         ret = mh.selectone()
    #         if not ret:
    #             print "**************begin word*****************"
    #             print time.time()
    #             # word信息存入数据库
    #             sql = "insert into post_index(`id`, `word`) values "
    #             word_values = ",".join(words.values())
    #             sql += word_values
    #             print "**************begin insert*****************"
    #             print time.time()
    #             mh.execute(sql)
    #             print "**************end insert*****************"
    #             print time.time()
    #             return
    #         for num in xrange(1000000):
    #             print num
    #             content = collections.defaultdict(int)
    #             # 对标题进行词频分析
    #             title_tmp = re.sub(seg, " ".decode("utf8"), ret[1])
    #             title_tmp = list(jieba.cut(title_tmp))
    #             for word in title_tmp:
    #                 # 标题词频：出现1次记为10次
    #                 content[word] += 10
    #             # 对内容进行词频分析
    #             content_tmp = re.sub(seg, " ".decode("utf8"), ret[2])
    #             content_tmp = list(jieba.cut(content_tmp))
    #             for word in content_tmp:
    #                 content[word] += 1
    #             # 进行整理倒排索引，更新或插入数据库
    #             for word, val in content.items():
    #                 if word == u' ':
    #                     continue
    #                 try:
    #                     word_id = eval(words[word])[0]
    #                 except:
    #                     word_num += 1
    #                     word_id = word_num
    #                     words[word] = str((word_num, word.encode("utf-8")))
    #                 insert_index.append(" ".join([str(word_id), str(ret[0]), str(val)]))
    #             ret = mh.selectone()
    #             if not ret:
    #                 break
    #         # index信息存入文档
    #         print "**************begin index  %s*****************" % i
    #         print time.time()
    #         f = open('%s.txt' % i, 'w')
    #         insert_index = sorted(insert_index, key=lambda x: int(x.split(" ")[0]))
    #         f.write(json.dumps(insert_index))
    #         f.close()

    @staticmethod
    def create_index():
        mh = get_mysql()
        seg = ur"[\s+\.\!\/\"\'\-\|\]\[\{\}\\]+|[+——！，。：、~@#￥%……&*（）；－):①②③④⑤⑥⑦⑧⑨⑩⑴⑵⑶⑷⑸⑹⑺─┅┆┈┍┎┒┙├┝┟┣┫┮┰┱┾┿╃╄╆╈╋▉▲▼※Ⅱ←→↖↗↘↙《》┊┋┇┉┠┨┌┐┑└┖┘┚┤┥┦┏┓┗┛┯┷┻┳┃━〓¤`˙⊙│〉〈⒂～？．＂”“’·■／￣�︱〕〔【】』『」「◎○◆●☉　┬┴┸┼═╔╗╚╝╩╭╮╯╰╱╲▁▅▆▇█▊▌▍▎▓▕□▽◇◢◣◤◥★☆︶︻︼︵︿﹃﹎﹏△▔▏▋▄▃▂△▔▏▋▄▃▂╳╬╫╪╨╧╦╥╣╢╠╟╜╙╘╖╓║╅╂┭┕┄〞〝〗〖〒〇〃⿻⿺⿹⿸⿷⿵⿴⿲]+"

        # 获取post数据
        sql = "select id, title, content from post"
        rets = mh.select(sql)

        insert_index = {}
        num = 0
        for ret in rets:
            num += 1
            print num
            content = collections.defaultdict(int)
            # 对标题进行词频分析
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
            # 进行整理倒排索引，更新或插入数据库
            for word, val in content.items():
                if word == u' ' or word == ' ':
                    continue
                try:
                    tmp = insert_index[word].split(",", 2)
                    insert_index[word] = "('%s'," % word + str(int(tmp[1])+1) + ",'" + tmp[2].strip('}\')') + ",\"" + str(int(ret[0])) + "\":" + str(val) + "}')"
                except:
                    insert_index[word] = "('%s'" % word + ",1,'{\"" + str(int(ret[0])) + "\":" + str(val) + "}')"

        # index信息存入文档
        print "**************begin index*****************"
        print time.time()
        sql = "insert into post_index(word, doc_fre, list) values "
        word_values = ",".join(insert_index.values())
        sql += word_values
        mh.execute(sql)
        print "**************end index*****************"
        print time.time()

if __name__ == '__main__':
    time1 = time.time()
    post_index().create_index()
    print time.time()-time1
