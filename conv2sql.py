import sqlite3
import json
import os
import mysql.connector


def  union_sql():
    conn = mysql.connector.connect(user='root', database='chengyu', use_unicode=True)
    cursor = conn.cursor()
    cursor.execute('select * from chengyu')
    while True:
        row = cursor.fetchone()
        if row:
            print(tuple(row))
        else:
            break
    cursor.close()
    conn.close()
    pass
# union_sql()


def  union_sql2():
    conn = mysql.connector.connect(user='root', database='dntown')
    cursor = conn.cursor()
    cursor.execute('select * from dntown_wx_idiom_list')
    # for (id,word,word_begin,word_begin_spell,word_end,word_end_spell,spell,content,derivation,sample) in cursor:
    for row in cursor:
        print([i.decode('utf-8') for i in row if type(i) == bytearray])
        # print(content.decode('utf-8'))
        # print(derivation)
    # while True:
    #     row = cursor.fetchone()
    #     if row:
    #         t = tuple(row)
    #         print(t)
    #     else:
    #         break
    cursor.close()
    conn.close()
    pass
union_sql2()

def import_sql(sql_file):
    root,ext = os.path.splitext(sql_file)
    conn = sqlite3.connect(root + '.db')

    c = conn.cursor()
    out_file = open(root + '.out','w')
    with open(sql_file, 'r') as in_file:
        while True:
            line = in_file.readline()
            line = line.strip()
            if line:
                try:
                    c.execute(line)
                except sqlite3.Error as e:
                    out_file.write(line+'\r\n')
                    print(e,line)
            else:
                break
    conn.commit()
    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()

    pass

# import_sql('dntown.sql')

def create_idiom():
    conn = sqlite3.connect('idiom.db')
    c = conn.cursor()
    # Create table

    # members = ['成语', '拼音', '简拼', '近义词', '反义词', '用法', '解释', '出处', '例子', '谒后语', '谜语', '成语故事']
    c.execute('''CREATE TABLE idiom
                 (chengyu TEXT,pinyin TEXT, jianpin TEXT, jinyi TEXT,
                 fanyi TEXT,yongfa TEXT,jieshi TEXT,chuchu TEXT,
                 lizi TEXT,xiehouyu TEXT,miyu TEXT,gushi TEXT)''')

    # c.execute("INSERT INTO idiom VALUES (?, ?)", (who, age))
    file_path = os.path.abspath(__file__ + "/../spider_poetry/spiders/idiom.json")
    with open(file_path, 'r') as in_file:
        while True:
            line = in_file.readline()
            line = line.strip()
            if line:
                idiom = eval(line)
                c.execute("INSERT INTO idiom VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                          (idiom.get('成语', ''),
                           idiom.get('拼音', ''),
                           idiom.get('简拼', ''),
                           idiom.get('近义词', ''),
                           idiom.get('反义词', ''),
                           idiom.get('用法', ''),
                           idiom.get('解释', ''),
                           idiom.get('出处', ''),
                           idiom.get('例子', ''),
                           idiom.get('谒后语', ''),
                           idiom.get('谜语', ''),
                           idiom.get('成语故事', ''),))
                pass
            else:
                break
    conn.commit()
    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()


# create_idiom()
