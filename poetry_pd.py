import sqlite3
import difflib
import json
import os

author50 = ['李白', '白居易', '杜甫', '苏轼', '辛弃疾', '李清照', '王维', '刘禹锡', '李商隐', '纳兰性德', '杜牧', '陆游', '陶渊明', '孟浩然', '李煜', '元稹',
            '柳宗元', '岑参', '王安石', '韩愈', '刘克庄', '王昌龄', '张籍', '孟郊', '杨万里', '文天祥', '欧阳修', '贾岛', '韦应物', '柳永', '温庭筠', '刘长卿',
            '张九龄', '周邦彦', '晏几道', '钱起', '秦观', '朱熹', '高适', '皮日休', '王之涣', '贺知章', '姜夔', '宋之问', '范成大', '范仲淹', '王勃', '晏殊',
            '岳飞', '曹操']


def calc_popular():
    #   计算流行度，流行度越高越容易
    #   默认是0 有注释+1 有tag+1 有译文+1 小学 +10 早教+10 初中+8 高中+5
    #   作者排名前10加3 前30加2 前50加1
    #   诗人李杜白的前10首加2 排名前10诗人前5首加2  排名前30诗人前两首 加2 排名前50诗人前1首 加2 代表作
    #   唐诗300 加3 宋词精选加2 宋词300 加1 古诗300加1
    #   属于名句加3
    conn = sqlite3.connect('poetries.db')

    def process_tag(conn, tag, popular):
        index_c = conn.cursor()
        index_c.execute('SELECT url FROM poetry_index where tag=?', (tag,))
        poetry_c = conn.cursor()
        urls = set()
        for url, in index_c:
            if url not in urls:
                urls.add(url)
                poetry_c.execute('update poetries_bk5 set popular=popular+? where url=?', (popular, url))
        conn.commit()

    def process_item(conn):
        author3 = author50[0:3]
        author30 = author50[3:30]
        index_c = conn.cursor()
        index_c.execute('SELECT author,url,tag,annotation,translation FROM poetries')
        poetry_c = conn.cursor()
        for author, url, tag, annotation, translation in index_c:
            score = 0
            if author in author3:
                score = 3
            elif author in author30:
                score = 2
            elif author in author50:
                score = 1
            if tag:
                score += 1
            if annotation:
                score += 1
            if translation:
                score += 1
            if score:
                poetry_c.execute('update poetries_bk5 set popular=popular+? where url=?', (score, url))
        conn.commit()

    def process_author_poetry(conn):
        for index, author in enumerate(author50):
            limit = 1
            if index < 3:
                limit = 10
            elif index < 10:
                limit = 5
            elif index < 30:
                limit = 2
            poetry_c = conn.cursor()
            index_c = conn.cursor()
            index_c.execute('SELECT url FROM poetry_index where tag="byauthor" and author=? order by pos LIMIT ?',
                            (author, limit))

            for url, in index_c:
                poetry_c.execute('update poetries_bk5 set popular=popular+2 where url=?', (url,))
        conn.commit()

    # process_tag(conn, 'xiaoxue', 10)
    # process_tag(conn, 'zaojiao100', 10)
    # process_tag(conn, 'chuzhong', 8)
    # process_tag(conn, 'gaozhong', 5)
    # process_tag(conn, 'songcijingxuan', 2)
    # process_tag(conn, 'songci300', 1)
    # process_tag(conn, 'tangshi300', 3)
    # process_tag(conn, 'gushi300', 1)
    # process_tag(conn, 'mingju', 3)
    # process_item(conn)
    # process_author_poetry(conn)

    # print(len(author50))


# calc_popular()


# def test():
#     conn = sqlite3.connect('poetries.db')
#     index_c = conn.cursor()
#     index_c.execute('SELECT ROWID FROM poetries limit 100')
#     for id, in index_c:
#         print(id)
#
#
# test()

def process_full_text():
    import re
    conn = sqlite3.connect('poetries.db')
    poetry_c = conn.cursor()
    index_c = conn.cursor()
    index_c.execute('SELECT id,full_text FROM poetries ')
    for id, text in index_c:
        full_text = re.sub(r'[（(].+[)）]', '', text, flags=re.DOTALL)
        poetry_c.execute('update poetries set full_text=? where id=?', (full_text, id))
    conn.commit()


# process_full_text()

def split_smart(sents):
    if not sents:
        return '', ''
    if len(sents) == 1:
        return sents[0], ''
    elif len(sents) == 2:
        return sents[0], sents[1]
    else:
        if len(sents[-1]) >= len(sents[0]):
            return '，'.join(sents[:-1]), sents[-1]
        else:
            return sents[0], '，'.join(sents[1:])


def split_sentence():
    import re
    conn = sqlite3.connect('poetries.db')
    ques_c = conn.cursor()
    ques_c.execute(
        '''CREATE TABLE IF NOT EXISTS questions 
          (poetry_id INT, pre_sent TEXT,next_sent TEXT);''')

    index_c = conn.cursor()
    index_c.execute('SELECT id,full_text FROM poetries')
    # LIMIT 200,350
    for id, text in index_c:
        clean_text = re.sub(r'["“”]', '', text)
        paras = [p.strip() for p in re.split(r'\r\n', clean_text) if p.strip()]
        for p in paras:
            # print('raw:', p)
            sents = [s.strip() for s in re.split(r'[?。？！!；]', p) if s.strip()]
            pre_sen = ''
            for sent in sents:
                pair = sent.split('，')
                if len(pair) >= 2:
                    sen1, sen2 = split_smart(pair)

                    if len(sen1) >= 4 and len(sen2) >= 4:
                        ques_c.execute('insert into questions (poetry_id,pre_sent,next_sent) values (?,?,?)',
                                       (id, sen1, sen2))
                        print(sen1, sen2)
                        pre_sen = ''
                        continue
                if pre_sen:
                    print(pre_sen, sent)
                    if len(pre_sen) >= 4 and len(sent) >= 4:
                        ques_c.execute('insert into questions (poetry_id,pre_sent,next_sent) values (?,?,?)',
                                       (id, pre_sen, sent))
                    pre_sen = ''
                else:
                    pre_sen = sent
    conn.commit()


def get_id_by_url(conn, url):
    c = conn.cursor()
    c.execute('SELECT id FROM poetries where url=?', (url,))
    ret = -1
    for id, in c:
        ret = id
        break
    return ret


def process_mingju():
    import re
    conn = sqlite3.connect('poetries.db')
    ques_c = conn.cursor()
    ques_c.execute(
        '''CREATE TABLE IF NOT EXISTS mingju_questions 
          (poetry_id INT, pre_sent TEXT,next_sent TEXT);''')
    index_c = conn.cursor()
    index_c.execute('SELECT url,mingju FROM poetry_index where tag=?', ('mingju',))
    mingju_set = set()
    for url, mingju in index_c:
        if mingju not in mingju_set:
            mingju_set.add(mingju)

            sents = [s.strip() for s in re.split(r'[?。？！!；]', mingju) if s.strip()]
            if len(sents) == 1:
                sents = sents[0].split('，')
            pre_sen, sen = split_smart(sents)
            ques_c.execute('insert into mingju_questions (poetry_id,pre_sent,next_sent) values (?,?,?)',
                           (get_id_by_url(conn, url), pre_sen, sen))
    conn.commit()


def create_question():
    conn = sqlite3.connect('poetries.db')
    ques_c = conn.cursor()
    ques_c.execute(
        '''CREATE TABLE IF NOT EXISTS mid_questions 
          (poetry_id INT, pre_sent TEXT,next_sent TEXT);''')

    questions = {id: list() for id in range(1, 5160)}
    mj_questions = {id: list() for id in range(1, 5160)}

    s_c = conn.cursor()
    s_c.execute('select * from questions order by poetry_id')
    for p_id, pre_sent, next_sent in s_c:
        questions[p_id].append([pre_sent, next_sent])

    s_c.execute('select * from mingju_questions where poetry_id>0 order by poetry_id')
    for p_id, pre_sent, next_sent in s_c:
        mj_questions[p_id].append([pre_sent, next_sent])

    pre_set, next_set = set()
    for p_id in range(1, 5160):
        for q in select_question(p_id, questions[p_id], mj_questions[p_id]):
            ques_c.execute('insert into mid_questions (poetry_id,pre_sent,next_sent) values (?,?,?)',
                           (p_id, q[0], q[1]))
    conn.commit()
    # print(questions)
    # print(mj_questions)


def select_question(p_id, questions, mj_questions):
    import functools
    mj = ''.join(functools.reduce(lambda x, y: x + y, mj_questions)) if mj_questions else ''

    def cmp_key(q):

        if len(q[0]) > 25 or len(q[1]) > 25 or len(q[0]) <= 3 or len(q[1]) <= 3:
            return 0
        return len(q[0]) + len(q[1]) + difflib.SequenceMatcher(None, mj, q[0] + q[1]).ratio() * 100 + random.uniform(1,
                                                                                                                     3)

    # if p_id <= 458:
    #     return questions

    # print(mj_questions)
    # print(questions)
    # questions = questions[::-1]
    questions.sort(key=cmp_key, reverse=True)
    # print(questions)
    # print('\r\n')
    count = 1
    if p_id <= 458:
        count = int(0.8 * len(questions))
        count = min(count, 10)
        count = max(5, count)
    elif p_id <= 1070:
        count = 3
    elif p_id <= 1978:
        count = 2
    return questions[0:max(count, len(mj_questions))]


import random


def select_options(p_id, question, questions, poetries, pre_index, next_index):
    type = 1
    if '，' in question[0]:
        type = 1
    elif '，' in question[1]:
        type = 2
    elif random.randint(1, 21) >= 14:
        type = 2
    # if p_id == 1 or p_id == 2:
    #     type = 2
    dynasty = poetries[p_id][1]
    author = poetries[p_id][0]
    title = poetries[p_id][2]
    tags = poetries[p_id][3]

    def calc_similarity(t1, t2, popular):
        if len(t2) > 10:
            return 0
        ratio = difflib.SequenceMatcher(None, t1, t2).ratio()
        if ratio >= 0.7:
            ratio = -1.0

        # 加上随机数，避免某个被总是选上
        return ratio * 200 + popular + random.uniform(1, 5)

    target = question[1] if type == 1 else question[0]
    target_index = pre_index if type == 2 else next_index
    target_len = len(target)

    set_target = set()
    for c in target:
        set_target = set_target | (target_index[c] & target_index[target_len])
    if len(set_target) <= 10:
        print(set_target, target_len, p_id)

    options = {}
    for t_id in set_target:
        id = int(t_id / 10000)
        ind = t_id % 10000
        if p_id == id:
            continue
        poetry = poetries[id]
        q = questions[id][ind]
        if not poetry or not q:
            continue
        ts = calc_similarity(target, q[1] if type == 1 else q[0], poetry[4])
        if id in options:
            if ts > options[id][0]:
                options[id][0] = ts
                options[id][1] = ind
        elif ts > 0:
            options.update({id: [ts, ind]})
    opt_list = []
    for k, v in options.items():
        v.append(k)
        opt_list.append(v)
    # options = [v.append(k) for k, v in options]
    opt_list.sort(key=lambda x: x[0], reverse=True)
    options = opt_list[:3]
    if len(options) != 3:
        return None

    grad = poetry[4]
    opt1_id, opt2_id, opt3_id = [opt[2] for opt in options]
    opt1_text, opt2_text, opt3_text = [questions[opt[2]][opt[1]][0 if type == 2 else 1] for opt in options]
    return question[0], question[
        1], dynasty, author, title, type, opt1_text, opt2_text, opt3_text, tags, grad, p_id, opt1_id, opt2_id, opt3_id
    # return
    # similarity = []
    # for id in range(1, len(questions) + 1):
    #     if p_id == id:
    #         similarity.append([-10000, id, -1])
    #     else:
    #         poetry = poetries[id]
    #         qs = questions[id]
    #         if not poetry or not qs:
    #             similarity.append([-10000, id, -1])
    #             continue
    #         simi, index = 0, -1
    #         for i, q in enumerate(qs):
    #             ts = calc_similarity(target, q[1] if type == 1 else q[0], poetry[4])
    #             if ts >= simi:
    #                 simi = ts
    #                 index = i
    #         similarity.append([simi, id, index])
    # options = similarity[0:3]
    # options.sort(key=lambda x: x[0], reverse=True)
    # for simi in similarity[3:]:
    #     if simi[0] > options[2][0]:
    #         options[2] = simi[::]
    #         options.sort(key=lambda x: x[0], reverse=True)

    # def max_cmp(id):
    #     if p_id == id:
    #         return 0
    #     if id in select_id:
    #         return 0

    ''


def create_options():
    conn = sqlite3.connect('poetries.db')
    ques_c = conn.cursor()
    ques_c.execute(
        '''CREATE TABLE IF NOT EXISTS re_questions 
          (id INTEGER PRIMARY KEY, pre_sent TEXT,sent TEXT,dynasty TEXT,author TEXT,title TEXT,type INT,
          option1 TEXT,option2 TEXT,option3 TEXT,tags TEXT,grad INT,
          poetry_id INT,opt1_p_id INT,opt2_p_id INT,opt3_p_id INT);''')

    rg = 5160

    pre_index = {}
    next_index = {}

    def create_index(type, text, p_id, index):
        tag_index = pre_index if type == 1 else next_index

        def update_index(tag_idx, key, val):
            if key not in tag_idx:
                c_set = set()
                tag_idx.update({key: c_set})
            else:
                c_set = tag_index[key]
            c_set.add(val)

        value = p_id * 10000 + index
        for key in text:
            update_index(tag_index, key, value)
        update_index(tag_index, len(text), value)

    questions = {id: list() for id in range(1, rg)}
    mj_questions = {id: list() for id in range(1, rg)}
    poetries = {id: list() for id in range(1, rg)}
    s_c = conn.cursor()
    s_c.execute('select * from questions order by poetry_id')

    for p_id, pre_sent, next_sent in s_c:
        create_index(1, pre_sent, int(p_id), len(questions[p_id]))
        create_index(2, next_sent, int(p_id), len(questions[p_id]))

        questions[p_id].append([pre_sent, next_sent])

    s_c.execute('select * from mingju_questions where poetry_id>0 order by poetry_id')
    for p_id, pre_sent, next_sent in s_c:
        mj_questions[p_id].append([pre_sent, next_sent])

    s_c.execute('select id,author,dynasty,title,tag,popular from poetries')
    for p_id, author, dynasty, title, tag, popular in s_c:
        poetries[p_id] = [author, dynasty, title, tag, popular]

    for p_id in range(1, rg):
        for q in select_question(p_id, questions[p_id], mj_questions[p_id]):
            res = select_options(p_id, q, questions, poetries, pre_index, next_index)
            if not res:
                continue
            # if p_id in range(1, 501):
            #     print(res)
            # else:
            #     print(p_id)
            # ''
            print(res)
            ques_c.execute(
                '''insert into re_questions (pre_sent ,sent ,dynasty ,author ,
                                              title ,type ,option1 ,option2 ,option3 ,tags ,grad ,
                                              poetry_id ,opt1_p_id ,opt2_p_id ,opt3_p_id) 
                                      values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                res)
    conn.commit()


def create_db():
    sconn = sqlite3.connect('poetries.db')
    tconn = sqlite3.connect('sanyu_poetry.db')
    tc = tconn.cursor()
    tc.execute(
        '''CREATE TABLE IF NOT EXISTS poetry 
          (id INTEGER PRIMARY KEY,dynasty TEXT, author TEXT, title TEXT,
          annotation TEXT, full_text TEXT,translation TEXT,popularity INT, tags TEXT,url TEXT);''')
    tc.execute(
        '''CREATE TABLE IF NOT EXISTS question 
          (id INTEGER PRIMARY KEY, stem TEXT,dynasty TEXT,author TEXT,title TEXT,type INT,
          option1 TEXT,option2 TEXT,option3 TEXT,option4 TEXT,
          answer INT,reference_id INT,tags TEXT,popularity INT);''')

    poetry_d = dict()

    sc = sconn.cursor()
    sc.execute('select * from poetries')
    for id, author, dynasty, title, url, tag, full_text, annotation, translation, popularity in sc:
        poetry_d.update({id: popularity})
        tc.execute('''insert into poetry values (?,?,?,?,?,?,?,?,?,?)''',
                   (id, dynasty, author, title, annotation, full_text, translation, popularity, tag, url))
    tconn.commit()

    sc.execute(
        'select id,pre_sent,sent,dynasty,author,title,type, option1,option2,option3,tags,poetry_id from re_questions')
    for id, pre_sent, sent, dynasty, author, title, type, option1, option2, option3, tags, poetry_id in sc:
        stem = pre_sent if type == 1 else sent
        option4 = pre_sent if type == 2 else sent
        options = [option1, option2, option3, option4]
        random.shuffle(options)
        answer = options.index(option4)
        popularity = poetry_d[poetry_id]
        tc.execute('''insert into question values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                   (id, stem, dynasty, author, title, type, *options, answer, poetry_id, tags, popularity))
    tconn.commit()


# split_sentence()


# create_question()

# create_options()

create_db()
