import re
import os
import scrapy
import sqlite3
from scrapy.http import HtmlResponse
import requests


class PoetryIndexItem():
    def __init__(self, author, title, url, tag, mingju, pos):
        self.author = author
        self.title = title
        self.url = url

        self.tag = tag
        self.mingju = mingju
        # 出现在第几页第几个位置，用于辅助测试
        self.pos = pos

        # print(author, title, url, tag, mingju, pos)

    def __hash__(self):
        return '%s_%s' % (self.author, self.title)

    def get_sign(self):
        return '%s_%s_%s' % (self.author.strip(), self.title.strip(), self.mingju.strip())


class Poetry:
    def __init__(self, author, dynasty, title, url, tag, full_text, annotation, translation):
        self.author = author.strip()
        self.dynasty = dynasty.strip()
        self.url = url
        self.title = re.sub(r'/.+?', '', title)
        self.tag = ','.join(tag)
        self.full_text = re.sub(r'（.+?）|\(.+?\)|《|》', '', '\r\n'.join((item.strip() for item in full_text)))
        self.annotation = '\r\n'.join((item.strip() for item in annotation if not re.fullmatch(r'^\(.+?\)$', item)))
        self.translation = '\r\n'.join((item.strip() for item in translation))

        # print(self.author, self.dynasty, self.url, self.title, self.tag, self.full_text, self.annotation,
        #       self.translation)


class PoetrySpider(scrapy.Spider):
    name = 'poetry'

    author_title_p = re.compile('^(.+?)《(.+?)》$')

    # start_urls = ["https://so.gushiwen.org/mingju/Default.aspx?p={0}&c=&t=".format(i) for i in range(1, 201)]

    def __init__(self):

        db_file = os.path.abspath(__file__ + "/../../../poetries.db")
        self.conn = sqlite3.connect(db_file)
        cursor = self.conn.cursor()
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS poetry_index (author TEXT, title TEXT, url TEXT, tag TEXT, mingju TEXT, pos TEXT);')
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS author_index (author TEXT, url TEXT,pos TEXT);')

        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS poetries 
              (author TEXT, dynasty TEXT,title TEXT,url TEXT,tag TEXT,full_text TEXT,annotation TEXT,translation TEXT);''')

        self.poetry_items = set()

        self.url_anno_tran = dict()
        self.url_tag = dict()
        self.url_title = dict()

    # def __del__(self):
    #     c = self.conn.cursor()
    #
    #     for k, v in self.url_anno_tran.items():
    #         translation = v.get('tran_yz', None) or v.get('tran_y', None) or v.get('tran', None) or []
    #         annotation = v.get('anno_yz', None) or v.get('anno_z', None) or v.get('anno', None) or []
    #         annotation = '\r\n'.join((item.strip() for item in annotation if not re.fullmatch(r'^\(.+?\)$', item)))
    #         translation = '\r\n'.join((item.strip() for item in translation))
    #         c.execute('update poetries set annotation = ?, translation = ? where url = ?', (annotation, translation, k))
    #         print(k, translation, annotation)
    #     self.conn.commit()

    # def __del__(self):
    #     c = self.conn.cursor()
    #
    #     for k, v in self.url_tag.items():
    #         c.execute('update poetries set tag = ? where url = ?', (v, k))
    #         print(k, v)
    #     self.conn.commit()

    def __del__(self):
        c = self.conn.cursor()

        for k, v in self.url_title.items():
            c.execute('update poetries_bk5 set title = ? where url = ?', (v, k))
            print(k, v)
        self.conn.commit()

    # 名句
    # def start_requests(self):
    #     for i in range(1, 201):
    #         yield scrapy.FormRequest("https://so.gushiwen.org/mingju/Default.aspx?p={0}&c=&t=".format(i),
    #                                  callback=self.parse, meta={'page': i})
    # yield "https://so.gushiwen.org/mingju/Default.aspx?p={0}&c=&t=".format(i)

    # 分类
    # def start_requests(self):
    #     lianjie = (
    #         ('https://so.gushiwen.org/gushi/xiaoxue.aspx', 'xiaoxue'),
    #         ('https://so.gushiwen.org/gushi/chuzhong.aspx', 'chuzhong'),
    #         ('https://so.gushiwen.org/gushi/sanbai.aspx', 'gushi300'),
    #         ('https://so.gushiwen.org/gushi/tangshi.aspx', 'tangshi300'),
    #         ('https://so.gushiwen.org/gushi/gaozhong.aspx', 'gaozhong'),
    #         ('https://so.gushiwen.org/gushi/songsan.aspx', 'songci300'),
    #         ('https://so.gushiwen.org/gushi/songci.aspx', 'songcijingxuan'),
    #         ('https://so.gushiwen.org/gushi/shijiu.aspx', 'gushi19'),
    #     )
    #     for lj in lianjie:
    #         yield scrapy.FormRequest(lj[0],
    #                                  callback=self.parse2, meta={'tag': lj[1]})

    # 早教100首
    # def start_requests(self):
    #     for i in range(1, 11):
    #         yield scrapy.FormRequest('https://www.gushiwen.org/shiwen/default_1A1036A{0}.aspx'.format(i),
    #                                  callback=self.parse3)

    # 作者
    # def start_requests(self):
    #     for i in range(1, 355):
    #         yield scrapy.FormRequest('https://so.gushiwen.org/authors/default.aspx?p={0}&c=不限'.format(i),
    #                                  callback=self.parse4, meta={'page': i})

    # 基于作者取诗
    # def start_requests(self):
    #     c = self.conn.cursor()
    #     c.execute('select url from author_index order by pos LIMIT 50')
    #     index = 1
    #     for url, in c:
    #         count_page = 10
    #         if index > 10 and index <= 30:
    #             count_page = 4
    #         elif index > 30:
    #             count_page = 2
    #         index += 1
    #         for page_index in range(1, count_page + 1):
    #             page_url = url[0:-6] + ('%d.aspx' % page_index)
    #             yield scrapy.FormRequest(page_url, callback=self.parse5, meta={'page': page_index})

    # for i in range(1, 355):
    #     yield scrapy.FormRequest('https://so.gushiwen.org/authors/default.aspx?p={0}&c=不限'.format(i),
    #                              callback=self.parse4, meta={'page': i})

    # def start_requests(self):
    #     # yield scrapy.FormRequest('https://so.gushiwen.org/shiwenv_a3ab60f1f510.aspx', callback=self.parse6)
    #     # return
    #     urls = set()
    #     c = self.conn.cursor()
    #     c.execute('select url from poetry_index')
    #
    #     for url, in c:
    #         if url not in urls:
    #             urls.add(url)
    #             yield scrapy.FormRequest(url, callback=self.parse6)

    # def start_requests(self):
    #     # yield scrapy.FormRequest('https://so.gushiwen.org/shiwenv_a3ab60f1f510.aspx', callback=self.parse6)
    #     # return
    #
    #     c = self.conn.cursor()
    #     c.execute('select url from poetries')
    #     urls = {url for url, in c}
    #     c.execute('select url from poetry_index')
    #
    #     for url, in c:
    #         if url not in urls:
    #             urls.add(url)
    #             yield scrapy.FormRequest(url, callback=self.parse6)

    # def start_requests(self):
    #     # self.url_anno_tran.update({'https://so.gushiwen.org/shiwenv_bc9f73652658.aspx': dict()})
    #     # yield scrapy.FormRequest('https://so.gushiwen.org/shiwenv_bc9f73652658.aspx', callback=self.parse8)
    #     #
    #     # return
    #
    #     c = self.conn.cursor()
    #     c.execute('select url from poetries where annotation="" or translation=""')
    #     index = 0
    #     for url, in c:
    #         self.url_anno_tran.update({url: dict()})
    #         yield scrapy.FormRequest(url, callback=self.parse8)
    #         index += 1
    #         # if index == 50:
    #         #     break

    # def start_requests(self):
    #     # self.url_anno_tran.update({'https://so.gushiwen.org/shiwenv_bc9f73652658.aspx': dict()})
    #     # yield scrapy.FormRequest('https://so.gushiwen.org/shiwenv_bc9f73652658.aspx', callback=self.parse8)
    #     #
    #     # return
    #
    #     c = self.conn.cursor()
    #     c.execute('select url from poetries')
    #     index = 0
    #     for url, in c:
    #         self.url_tag.update({url: ''})
    #         yield scrapy.FormRequest(url, callback=self.parse9)
    #         index += 1
    #         # if index == 50:
    #         #     break

    def start_requests(self):
        # self.url_anno_tran.update({'https://so.gushiwen.org/shiwenv_bc9f73652658.aspx': dict()})
        # yield scrapy.FormRequest('https://so.gushiwen.org/shiwenv_bc9f73652658.aspx', callback=self.parse8)
        #
        # return

        c = self.conn.cursor()
        c.execute('select url from poetries_bk5')
        index = 0
        for url, in c:
            self.url_title.update({url: ''})
            yield scrapy.FormRequest(url, callback=self.parse10)
            index += 1
            # if index == 50:
            #     break

    def parse10(self, response):
        sel = response.xpath('//div[@class="left"]/div[@class="sons"]/div[@class="cont"]')[0]
        title = sel.xpath('./h1/text()').extract_first()

        self.url_title[response.url] = re.sub(r'/.+$|（.+）', '', title).strip()

    def parse9(self, response):
        tag = response.xpath('//div[@class="left"]/div[@class="sons"]')[0].xpath(
            './div[@class="tag"]/a/text()').extract()
        self.url_tag[response.url] = ','.join(tag)

    def parse_yiwen(self, response):
        # print('yiwen')
        url = response.meta['url']
        translation = response.xpath('.//p/span/text()').extract()
        if translation:
            self.url_anno_tran[url].update(dict(tran_y=translation))

    def parse_zhushi(self, response):
        # print('zhushi')
        url = response.meta['url']
        annotation = response.xpath('.//p/span/text()').extract()
        if annotation:
            self.url_anno_tran[url].update(dict(anno_z=annotation))

    def parse_yizhu(self, response):
        # print('yizhu')
        translation = []
        annotation = []
        url = response.meta['url']
        yizhu = response.xpath('.//div[@class="contyishang"]/p')

        if yizhu:
            translation = yizhu[0].xpath('./text()').extract()
            annotation = yizhu[-1].xpath('./text()').extract()

        if translation:
            self.url_anno_tran[url].update(dict(tran_yz=translation))

        if annotation:
            self.url_anno_tran[url].update(dict(anno_yz=annotation))

    def parse8(self, response):

        annotation = []
        translation = []

        yishangs = response.xpath('//div[@class="left"]/div[@class="sons"]/div[@class="contyishang"]')
        for ys in yishangs:
            type = ys.xpath('./div/h2/span/text()').extract_first()
            if type == '译文及注释' or type == '注释' or type == '译文及注释2':
                url = ys.xpath('./div/a/@href').extract()
                if len(url) == 2:
                    id = re.sub("\D", "", url[1])
                    yield response.follow('/shiwen2017/ajaxfanyi.aspx?id=' + id, self.parse_yizhu,
                                          meta={'url': response.url})

                yizhu = ys.xpath('./p')
                if yizhu:
                    translation = yizhu[0].xpath('./text()').extract()
                    annotation = yizhu[-1].xpath('./text()').extract()
                break

        id = response.xpath(
            '//div[@class="left"]/div[@class="sons"]/div[@class="cont"]/div[@class="yizhu"]/img/@onclick').extract_first()
        id = re.sub(r'.+?\(\'|\'\)', '', id)
        yiwen_url = '/shiwen2017/ajaxshiwencont.aspx?id=' + id + '&value=yi'
        zhushi_url = '/shiwen2017/ajaxshiwencont.aspx?id=' + id + '&value=zhu'

        yield response.follow(yiwen_url, self.parse_yiwen, meta={'url': response.url})

        yield response.follow(zhushi_url, self.parse_zhushi, meta={'url': response.url})

        if translation:
            self.url_anno_tran[response.url].update(dict(tran=translation))

        if annotation:
            self.url_anno_tran[response.url].update(dict(anno=annotation))

        # p_item = Poetry(author, dynasty, title, response.url, tag, full_text, annotation, translation)

    def parse7(self, response):
        author = ''
        dynasty = ''
        title = ''
        tag = []
        full_text = []
        annotation = []
        translation = []

        if not annotation:
            yishangs = response.xpath('//div[@class="left"]/div[@class="sons"]/div[@class="contyishang"]')
            for ys in yishangs:
                type = ys.xpath('./div/h2/span/text()').extract_first()
                if type == '译文及注释' or type == '注释' or type == '译文及注释2':
                    url = ys.xpath('./div/a/@href').extract()
                    if len(url) == 2:
                        id = re.sub("\D", "", url[1])
                        ys_temp = self.request_url(response.urljoin('/shiwen2017/ajaxfanyi.aspx?id=' + id)).xpath(
                            './/div[@class="contyishang"]')
                        if not ys_temp:
                            ys = ys_temp
                        # print(yw_resp.text())
                    yizhu = ys.xpath('./p')
                    if yizhu:
                        annotation = yizhu[-1].xpath('./text()').extract()

        id = response.xpath(
            '//div[@class="left"]/div[@class="sons"]/div[@class="cont"]/div[@class="yizhu"]/img/@onclick').extract_first()
        id = re.sub(r'.+?\(\'|\'\)', '', id)
        yiwen_url = '/shiwen2017/ajaxshiwencont.aspx?id=' + id + '&value=yi'
        zhushi_url = '/shiwen2017/ajaxshiwencont.aspx?id=' + id + '&value=zhu'

        if not annotation:
            zhushi = self.request_url(response.urljoin(zhushi_url))
            annotation = zhushi.xpath('.//p/span/text()').extract()
        if annotation:
            p_item = Poetry(author, dynasty, title, response.url, tag, full_text, annotation, translation)
            print('done', response.url, annotation)
            self.update_annotation(p_item)
        else:
            print('failed', response.url)

    def parse6(self, response):
        author = ''
        dynasty = ''
        title = ''
        tag = []
        full_text = []
        annotation = []
        translation = []
        sel = response.xpath('//div[@class="left"]/div[@class="sons"]/div[@class="cont"]')[0]
        title = sel.xpath('./h1/text()').extract_first()
        da = sel.xpath('./p[@class="source"]/a/text()').extract()
        full_text = sel.xpath('./div[@class="contson"]/p/text()').extract()
        if not full_text:
            full_text = sel.xpath('./div[@class="contson"]/text()').extract()
        dynasty = da[0]
        author = da[1]
        tag = response.xpath('//div[@class="left"]/div[@class="sons"]/div[@class="tag"]/a/text()').extract()

        if not translation and not annotation:
            yishangs = response.xpath('//div[@class="left"]/div[@class="sons"]/div[@class="contyishang"]')
            for ys in yishangs:
                type = ys.xpath('./div/h2/span/text()').extract_first()
                if type == '译文及注释' or type == '注释' or type == '译文及注释2':
                    url = ys.xpath('./div/a/@href').extract()
                    if len(url) == 2:
                        id = re.sub("\D", "", url[1])
                        ys_temp = self.request_url(response.urljoin('/shiwen2017/ajaxfanyi.aspx?id=' + id)).xpath(
                            './/div[@class="contyishang"]')
                        if not ys_temp:
                            ys = ys_temp
                        # print(yw_resp.text())
                    yizhu = ys.xpath('./p')
                    if yizhu:
                        translation = yizhu[0].xpath('./text()').extract()
                        annotation = yizhu[-1].xpath('./text()').extract()
                    break

        id = response.xpath(
            '//div[@class="left"]/div[@class="sons"]/div[@class="cont"]/div[@class="yizhu"]/img/@onclick').extract_first()
        id = re.sub(r'.+?\(\'|\'\)', '', id)
        yiwen_url = '/shiwen2017/ajaxshiwencont.aspx?id=' + id + '&value=yi'
        zhushi_url = '/shiwen2017/ajaxshiwencont.aspx?id=' + id + '&value=zhu'

        if not translation:
            yiwen = self.request_url(response.urljoin(yiwen_url))
            translation = yiwen.xpath('.//p/span/text()').extract()
        if not annotation:
            zhushi = self.request_url(response.urljoin(zhushi_url))
            annotation = zhushi.xpath('.//p/span/text()').extract()

        p_item = Poetry(author, dynasty, title, response.url, tag, full_text, annotation, translation)
        self.save_poetry(p_item)

    def update_annotation(self, item):
        c = self.conn.cursor()
        c.execute('update poetries_bk5 set annotation = ? where url = ?', (item.annotation, item.url))
        self.conn.commit()

    def update_translation(self, item):
        c = self.conn.cursor()
        c.execute('update poetries_bk5 set translation = ? where url = ?', (item.translation, item.url))
        self.conn.commit()

    def save_poetry(self, item):
        c = self.conn.cursor()
        c.execute('INSERT INTO poetries_bk5 VALUES (?,?,?,?,?,?,?,?)',
                  (item.author, item.dynasty, item.title, item.url, item.tag, item.full_text, item.annotation,
                   item.translation))
        self.conn.commit()
        # print(type)
        # print(title, dynasty, author, content, tag)

        # for index, sel in enumerate(sels):
        #     ps = sel.xpath('./p')
        #     p1 = ps[0]
        #     p2 = ps[1]
        #     title = p1.xpath('./a/b/text()').extract()[0].strip()
        #     author = p2.xpath('./a/text()').extract()[1].strip()
        #     url = response.urljoin(p1.xpath('./a/@href').extract()[0])
        #
        #     print(title, author, url)
        #     # p_item = PoetryItem(author, title, url, 'byauthor', '', '')
        #     # self.save_poetry_index(p_item)

    def request_url(self, url):
        # print('request url:', url)
        body = requests.get(url).text.encode('utf-8')
        response = HtmlResponse(url=url, body=body, headers={'Connection': 'close'})
        return response

    def parse5(self, response):
        sels = response.xpath('//div[@class="left"]/div[@class="sons"]/div[@class="cont"]')
        for index, sel in enumerate(sels):
            ps = sel.xpath('./p')
            p1 = ps[0]
            p2 = ps[1]
            title = p1.xpath('./a/b/text()').extract()[0].strip()
            author = p2.xpath('./a/text()').extract()[1].strip()
            url = response.urljoin(p1.xpath('./a/@href').extract()[0])
            pos = '%03d.%03d' % (response.meta['page'], index)
            p_item = PoetryIndexItem(author, title, url, 'byauthor', '', pos)
            self.save_poetry_index(p_item)

    def parse4(self, response):
        sels = response.xpath('//div[@class="left"]/div[@class="sonspic"]/div[@class="cont"]')
        for index, sel in enumerate(sels):
            author = sel.xpath('./p/a/b/text()').extract()[0].strip()
            url = response.urljoin(sel.xpath('./p/a/@href').extract()[2])
            pos = '%03d.%03d' % (response.meta['page'], index)
            self.save_author_index(author, url, pos)
            # print(author, url, pos)

    def parse3(self, response):
        sels = response.xpath('//div[@class="left"]/div[@class="sons"]/div[@class="cont"]')
        for sel in sels:
            ps = sel.xpath('./p')
            p1 = ps[0]
            p2 = ps[1]
            title = p1.xpath('./a/b/text()').extract()[0].strip()
            author = p2.xpath('./a/text()').extract()[1].strip()
            url = p1.xpath('./a/@href').extract()[0]
            p_item = PoetryIndexItem(author, title, url, 'zaojiao100', '', '')
            self.save_poetry_index(p_item)

    def parse2(self, response):
        sels = response.xpath('//div[@class="typecont"]/span')
        for sel in sels:
            title = sel.xpath('./a/text()').extract()[0].strip()
            url = response.urljoin(sel.xpath('./a/@href').extract()[0])
            author = sel.xpath('./text()').extract()
            author = author[0].strip('()》《 ') if author else ''
            p_item = PoetryIndexItem(author, title, url, response.meta['tag'], '', '')
            self.save_poetry_index(p_item)

    def parse(self, response):
        sels = response.xpath('//div[@class="left"]/div[@class="sons"]/div[@class="cont"]')
        for index, sel in enumerate(sels):
            content = sel.xpath('./a/text()').extract()
            if len(content) != 2:
                print('内容异常')
                continue

            mr = self.author_title_p.fullmatch(content[1])
            if mr is None:
                print('解析作者、标题异常')
                continue
            mr = mr.groups()
            if len(mr) != 2:
                print('作者、标题 解析错误')
                continue

            pos = '%03d.%03d' % (response.meta['page'], index)

            urls = sel.xpath('./a/@href').extract()

            if len(urls) != 2:
                print('url异常')
                continue
            url = response.urljoin(urls[1])

            p_item = PoetryIndexItem(mr[0].strip(), mr[1].strip(), url, 'mingju', content[0].strip(), pos)

            # if p_item.get_sign() in self.poetry_items:
            #     print(mr[0], mr[1], 'already exist')
            # else:
            self.poetry_items.add(p_item.get_sign())
            self.save_poetry_index(p_item)

    def save_poetry_index(self, item):
        c = self.conn.cursor()
        c.execute('INSERT INTO poetry_index VALUES (?,?,?,?,?,?)',
                  (item.author, item.title, item.url, item.tag, item.mingju, item.pos))
        self.conn.commit()

    def save_author_index(self, author, url, pos):
        c = self.conn.cursor()
        c.execute('INSERT INTO author_index VALUES (?,?,?)',
                  (author, url, pos))
        self.conn.commit()

'''
臣某言：伏以佛者，夷狄之一法耳，自后汉时流入中国,上古未尝有也。昔者黄帝在位百年，年百一十岁；少昊在位八十年，年百岁；颛顼在位七十九年，年九十八岁；帝喾在位七十年，年百五岁；帝尧在位九十八年，年百一十八岁；帝舜及禹，年皆百岁。此时天下太平，百姓安乐寿考，然而中国未有佛也。其后殷汤亦年百岁，汤孙太戊在位七十五年，武丁在位五十九年，书史不言其年寿所极，推其年数，盖亦俱不减百岁。周文王年九十七岁，武王年九十三岁，穆王在位百年。此时佛法亦未入中国，非因事佛而致然也。
汉明帝时，始有佛法，明帝在位，才十八年耳。其后乱亡相继，运祚不长。宋、齐、梁、陈、元魏已下，事佛渐谨，年代尤促。惟梁武帝在位四十八年，前后三度舍身施佛，宗庙之祭，不用牲牢，昼日一食，止于菜果，其后竞为侯景所逼，饿死台城，国亦寻灭。事佛求福，乃更得祸。由此观之，佛不足事，亦可知矣。
高祖始受隋禅，则议除之。当时群臣材识不远，不能深知先王之道，古今之宜，推阐圣明，以救斯弊，其事遂止，臣常恨焉。伏维睿圣文武皇帝陛下，神圣英武，数千百年已来，未有伦比。即位之初，即不许度人为僧尼道，又不许创立寺观。臣常以为高祖之志，必行于陛下之手，今纵未能即行，岂可恣之转令盛也?
今闻陛下令群僧迎佛骨于凤翔，御楼以观，舁入大内，又令诸寺递迎供养。臣虽至愚，必知陛下不惑于佛，作此崇奉，以祈福祥也。直以年丰人乐，徇人之心，为京都士庶设诡异之观，戏玩之具耳。安有圣明若此，而肯信此等事哉!然百姓愚冥，易惑难晓，苟见陛下如此，将谓真心事佛，皆云：“天子大圣，犹一心敬信；百姓何人，岂合更惜身命!”焚顶烧指，百十为群，解衣散钱，自朝至暮，转相仿效，惟恐后时，老少奔波，弃其业次。若不即加禁遏，更历诸寺，必有断臂脔身以为供养者。伤风败俗，传笑四方，非细事也。
夫佛本夷狄之人，与中国言语不通，衣服殊制；口不言先王之法言，身不服先王之法服；不知君臣之义，父子之情。假如其身至今尚在，奉其国命，来朝京师，陛下容而接之，不过宣政一见，礼宾一设，赐衣一袭，卫而出之于境，不令惑众也。况其身死已久，枯朽之骨，凶秽之馀，岂宜令入宫禁？
孔子曰：“敬鬼神而远之。”古之诸侯，行吊于其国，尚令巫祝先以桃茹祓除不祥，然后进吊。今无故取朽秽之物，亲临观之，巫祝不先，桃茹不用，群臣不言其非，御史不举其失，臣实耻之。乞以此骨付之有司，投诸水火，永绝根本，断天下之疑，绝后代之惑。使天下之人，知大圣人之所作为，出于寻常万万也。岂不盛哉!岂不快哉!佛如有灵，能作祸祟，凡有殃咎，宜加臣身，上天鉴临，臣不怨悔。无任感激恳悃之至，谨奉表以闻。臣某诚惶诚恐。
'''