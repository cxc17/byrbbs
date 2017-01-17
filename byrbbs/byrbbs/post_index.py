# coding: utf-8
# 建立、更新post索引
from mysqlclient import get_mysql
from SpiderConfig import SpiderConfig

import collections
import jieba
import re
import time


class post_index(object):
    def __init__(self):
        SpiderConfig.initialize()

    # @staticmethod
    # def update_stopwords():
    #     mh = get_mysql()
    #     sql = "select word, status from stop_word"
    #     rets = mh.select(sql)
    #     for ret in rets:
    #         sql = "select id, doc_fre from post_index where word='%s'" % ret[0]
    #         ret_words = mh.select(sql)
    #         for ret_word in ret_words:
    #             update_sql = "update post_index set doc_fre=%s where id='%s'" % (int(ret_word[1])*int(ret[1]), ret_word[0])
    #             mh.execute(update_sql)

    # 停用词相关操作
    @staticmethod
    def test():
        mh = get_mysql()
        sql = "select word from stop_word"
        rets = mh.select(sql)
        stop_word = []
        for ret in rets:
            stop_word.append(ret[0])
        import json
        sql = "insert into stop_word(word) values('%s')" % json.dumps(stop_word, ensure_ascii=False)
        mh.execute(sql)
        print stop_word

    # 更新停用词到数据库
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

    # 创建index
    @staticmethod
    def create_index(step, k):
        mh = get_mysql()
        seg = ur"[\s\.\!\/\"\'\-\|\]\[\{\}\\]+|[ⅹⅸⅷⅶⅵⅴⅳⅲⅱⅰ℡№℉℃‵″′‰‥‘яюэьыъщшчцхфутсрпонмлкйизжеёдгвбаωψχφυτσρποξνμλκιθηζεδγβαˋˊˉˇ÷×±°¨§￤￢￡￠｀＿＾ｚｙｘｗｖｕｔｓｒｑｐｏｎｍｌｋｊｉｈｇｆｅｄｃｂａ＠＞＝＜９８７６５４３２１０＋＊＇＆％＄＃﹫﹪﹦﹥﹤﹣﹢﹡﹠﹟﹞﹝﹜﹛﹚﹙﹗﹖﹕﹔﹒﹐﹍﹌﹋﹊﹉﹄﹂﹁﹀︾︽︺︹︸︷︴︰——！，。：、~@#￥%……&*（）；－):ǔūúüùɡǎāáàěêéёèēīìíǐōòóǒňǹńǖǘǚǜ①②③④⑤⑥⑦⑧⑨⑩⑴⑵⑶⑷⑸⑹⑺─┅┆┈┍┎┒┙├┝┟┣┫┮┰┱┾┿╃╄╆╈╋▉▲▼※Ⅱ←→↖↗↘↙《》┊┋┇┉┠┨┌┐┑└┖┘┚┤┥┦┏┓┗┛┯┷┻┳┃━〓¤`˙⊙│〉〈⒂～？．＂”“’·■／￣�︱〕〔【】』『」「◎○◆●☉　┬┴┸┼═╔╗╚╝╩╭╮╯╰╱╲▁▅▆▇█▊▌▍▎▓▕□▽◇◢◣◤◥★☆︶︻︼︵︿﹃﹎﹏△▔▏▋▄▃▂△▔▏▋▄▃▂╳╬╫╪╨╧╦╥╣╢╠╟╜╙╘╖╓║╅╂┭┕┄〞〝〗〖〒〇〃⿻⿺⿹⿸⿷⿵⿴⿲]+"

        # 获取post数据
        sql = "select id, title, content from post limit %s, %s" % (step*k, step)
        rets = mh.select(sql)

        insert_index = {}
        num = 0
        for ret in rets:
            num += 1
            if num % 1000 == 0:
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
                if word == u" " or word == " ":
                    continue
                word = word.lower()[:255]
                try:
                    tmp = insert_index[word].split(",", 2)
                    insert_index[word] = "('%s',%s,'%s,\"%s\":%s}')" % (word, str(int(tmp[1])+1), tmp[2].strip('}\')'),
                                                                        str(int(ret[0])), str(val))
                except:
                    insert_index[word] = "('%s',1,'{\"%s\":%s}')" % (word, str(int(ret[0])), str(val))

        # index信息存入文档
        print "**************begin index*****************"
        print time.time()
        insert_index = ",".join(insert_index.values())
        insert_index = "insert into post_index%s(word, doc_fre, list) values " % k + insert_index
        mh.execute(insert_index)
        print "**************end index*****************"
        print time.time()

    # 获取更新的index
    @staticmethod
    def update_index_delete():
        mh = get_mysql()

        seg = ur"[\s\.\!\/\"\'\-\|\]\[\{\}\\]+|[ⅹⅸⅷⅶⅵⅴⅳⅲⅱⅰ℡№℉℃‵″′‰‥‘яюэьыъщшчцхфутсрпонмлкйизжеёдгвбаωψχφυτσρποξνμλκιθηζεδγβαˋˊˉˇ÷×±°¨§￤￢￡￠｀＿＾ｚｙｘｗｖｕｔｓｒｑｐｏｎｍｌｋｊｉｈｇｆｅｄｃｂａ＠＞＝＜９８７６５４３２１０＋＊＇＆％＄＃﹫﹪﹦﹥﹤﹣﹢﹡﹠﹟﹞﹝﹜﹛﹚﹙﹗﹖﹕﹔﹒﹐﹍﹌﹋﹊﹉﹄﹂﹁﹀︾︽︺︹︸︷︴︰——！，。：、~@#￥%……&*（）；－):ǔūúüùɡǎāáàěêéёèēīìíǐōòóǒňǹńǖǘǚǜ①②③④⑤⑥⑦⑧⑨⑩⑴⑵⑶⑷⑸⑹⑺─┅┆┈┍┎┒┙├┝┟┣┫┮┰┱┾┿╃╄╆╈╋▉▲▼※Ⅱ←→↖↗↘↙《》┊┋┇┉┠┨┌┐┑└┖┘┚┤┥┦┏┓┗┛┯┷┻┳┃━〓¤`˙⊙│〉〈⒂～？．＂”“’·■／￣�︱〕〔【】』『」「◎○◆●☉　┬┴┸┼═╔╗╚╝╩╭╮╯╰╱╲▁▅▆▇█▊▌▍▎▓▕□▽◇◢◣◤◥★☆︶︻︼︵︿﹃﹎﹏△▔▏▋▄▃▂△▔▏▋▄▃▂╳╬╫╪╨╧╦╥╣╢╠╟╜╙╘╖╓║╅╂┭┕┄〞〝〗〖〒〇〃⿻⿺⿹⿸⿷⿵⿴⿲]+"
        # 删除post_index中已删除的post索引
        print "**************delete post_index*****************"
        sql = "select id, title, content from post_delete"
        rets = mh.select(sql)
        i = 0
        print len(rets)
        for ret in rets:
            i += 1
            total_content = []
            # 对标题进行词频分析
            title_tmp = re.sub(seg, " ".decode("utf8"), ret[1])
            title_tmp = list(jieba.cut(title_tmp))
            for word in title_tmp:
                total_content.append(word)
            # 对内容进行词频分析
            content_tmp = re.sub(seg, " ".decode("utf8"), ret[2])
            content_tmp = list(jieba.cut(content_tmp))
            for word in content_tmp:
                total_content.append(word)
            print i, len(total_content)

            for x in xrange(100):
                content = total_content[x*50:(x+1)*50]
                if not content:
                    break
                sql = "select id, word, doc_fre, list from post_index where word in (\"%s\")" % "\",\"".join(content)
                ret_indexs = mh.select(sql)
                sql = "delete from post_index where word in (\"%s\")" % "\",\"".join(content)
                mh.execute(sql)
                insert_index = []
                for ret_index in ret_indexs:
                    lists = re.sub(r",*\"%s\":\d+,*" % ret[0], ",".decode("utf8"), ret_index[3])\
                        .replace("{,", "{").replace(",}", "}")
                    if lists != "{}":
                        insert_index.append("(%s, '%s', %s, '%s')" % (ret_index[0], ret_index[1], int(ret_index[2]) - 1,
                                                                      lists))
                if not insert_index:
                    continue
                insert_index = ",".join(insert_index)
                insert_index = "insert into post_index(id, word, doc_fre, list) values " + insert_index
                mh.execute(insert_index)
        print "**************end post_index*****************"

    @staticmethod
    def post_index_update():
        mh = get_mysql()

        seg = ur"[\s\.\!\/\"\'\-\|\]\[\{\}\\]+|[ⅹⅸⅷⅶⅵⅴⅳⅲⅱⅰ℡№℉℃‵″′‰‥‘яюэьыъщшчцхфутсрпонмлкйизжеёдгвбаωψχφυτσρποξνμλκιθηζεδγβαˋˊˉˇ÷×±°¨§￤￢￡￠｀＿＾ｚｙｘｗｖｕｔｓｒｑｐｏｎｍｌｋｊｉｈｇｆｅｄｃｂａ＠＞＝＜９８７６５４３２１０＋＊＇＆％＄＃﹫﹪﹦﹥﹤﹣﹢﹡﹠﹟﹞﹝﹜﹛﹚﹙﹗﹖﹕﹔﹒﹐﹍﹌﹋﹊﹉﹄﹂﹁﹀︾︽︺︹︸︷︴︰——！，。：、~@#￥%……&*（）；－):ǔūúüùɡǎāáàěêéёèēīìíǐōòóǒňǹńǖǘǚǜ①②③④⑤⑥⑦⑧⑨⑩⑴⑵⑶⑷⑸⑹⑺─┅┆┈┍┎┒┙├┝┟┣┫┮┰┱┾┿╃╄╆╈╋▉▲▼※Ⅱ←→↖↗↘↙《》┊┋┇┉┠┨┌┐┑└┖┘┚┤┥┦┏┓┗┛┯┷┻┳┃━〓¤`˙⊙│〉〈⒂～？．＂”“’·■／￣�︱〕〔【】』『」「◎○◆●☉　┬┴┸┼═╔╗╚╝╩╭╮╯╰╱╲▁▅▆▇█▊▌▍▎▓▕□▽◇◢◣◤◥★☆︶︻︼︵︿﹃﹎﹏△▔▏▋▄▃▂△▔▏▋▄▃▂╳╬╫╪╨╧╦╥╣╢╠╟╜╙╘╖╓║╅╂┭┕┄〞〝〗〖〒〇〃⿻⿺⿹⿸⿷⿵⿴⿲]+"
        # post_index_update数据更新
        print "**************update post_index_update*****************"

        # 删除更新post_index原有数据
        sql = "delete from post_index_update"
        mh.execute(sql)

        # 删除更新post_index原有数据
        sql = "select post_id from post_last_id"
        post_last_id = mh.select(sql)[0][0]

        # 获取post数据
        sql = "select id, title, content from post where id > '%s'" % post_last_id
        rets = mh.select(sql)

        insert_index = {}
        for ret in rets:
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
                if word == u" " or word == " ":
                    continue
                word = word.lower()[:255]
                try:
                    tmp = insert_index[word].split(",", 2)
                    insert_index[word] = "('%s',%s,'%s,\"%s\":%s}')" % (word, str(int(tmp[1])+1), tmp[2].strip('}\')'),
                                                                        str(int(ret[0])), str(val))
                except:
                    insert_index[word] = "('%s',1,'{\"%s\":%s}')" % (word, str(int(ret[0])), str(val))

        # index信息存入文档
        print "**************begin index*****************"
        insert_index = ",".join(insert_index.values())
        insert_index = "insert into post_index_update(word, doc_fre, list) values " + insert_index
        mh.execute(insert_index)
        print "**************end index*****************"
        print "**************end post_index_update*****************"

    # 合并更新的index
    @staticmethod
    def update_merge_data():
        mh = get_mysql()
        for now in xrange(1000):
            sql = "SELECT post_index.id,post_index.word,post_index.doc_fre,post_index.list,post_index_update.doc_fre," \
                  "post_index_update.list from post_index join post_index_update on " \
                  "post_index.word = post_index_update.word limit %s, %s" % (1000*now, 1000)
            rets = mh.select(sql)
            if not rets:
                break
            print "**************update_index*****************"
            insert_index = []
            for ret in rets:
                insert_index.append("(%s, '%s', %s, '%s')" % (ret[0], ret[1], int(ret[2])+int(ret[4]),
                                                              ret[3].strip("}")+',' + ret[5].strip("{")))

            sql = "delete from post_index where id in (select id from (select post_index.id from post_index join " \
                  "post_index_update on post_index.word = post_index_update.word) as newtable)"
            mh.execute(sql)

            x = 10
            num = len(insert_index) / x
            for i in xrange(x+1):
                print "**************insert index %s*****************" % i
                insert_index_now = ",".join(insert_index[i*num:(i+1)*num])
                if not insert_index_now:
                    break
                insert_index_now = "insert into post_index(id, word, doc_fre, list) values " + insert_index_now
                mh.execute(insert_index_now)

        print "**************insert_into*****************"
        insert_sql = "INSERT into post_index(word, doc_fre, list) SELECT word, doc_fre, list from post_index_update " \
                     "where word not in (SELECT word from post_index)"
        mh.execute(insert_sql)

    # 合并index
    @staticmethod
    def merge_data(k):
        mh = get_mysql()
        sql = "select post_index.id,post_index.word,post_index.doc_fre,post_index.list,post_index%s.doc_fre," \
              "post_index%s.list from post_index join post_index%s on post_index.word = post_index%s.word" % (k, k, k, k)
        rets = mh.select(sql)

        print "**************update_index*****************"
        insert_index = []
        for ret in rets:
            insert_index.append("(%s, '%s', %s, '%s')" % (ret[0], ret[1], int(ret[2])+int(ret[4]), ret[3].strip("}") +
                                                          ',' + ret[5].strip("{")))

        sql = "delete from post_index where id in (select id from (select post_index.id from post_index join " \
              "post_index%s on post_index.word = post_index%s.word) as newtable)" % (k, k)
        mh.execute(sql)

        x = 20
        num = len(insert_index) / x
        for i in xrange(x+1):
            print "**************insert index %s*****************" % i
            insert_index_now = ",".join(insert_index[i*num:(i+1)*num])
            insert_index_now = "insert into post_index(id, word, doc_fre, list) values " + insert_index_now
            mh.execute(insert_index_now)

        print "**************insert_into*****************"
        insert_sql = "INSERT into post_index(word, doc_fre, list) select word, doc_fre, list from post_index%s " \
                     "where word not in (select word from post_index)" % k
        mh.execute(insert_sql)

    @staticmethod
    def all_post_index(step, k):
        for i in xrange(k, 13, 2):
            print "**************start %s post_index *****************" % i
            time1 = time.time()
            post_index().create_index(step, i)
            print time.time()-time1

    @staticmethod
    def all_merge_data():
        for i in xrange(1, 13):
            print "**************start %s post_index *****************" % i
            time1 = time.time()
            post_index().merge_data(i)
            print time.time()-time1

    # post_index更新
    @staticmethod
    def index_update():
        post_index().update_index_delete()
        # post_index().post_index_update()
        # post_index().update_merge_data()


if __name__ == '__main__':
    time1 = time.time()
    post_index().all_merge_data()
    print time.time()-time1

