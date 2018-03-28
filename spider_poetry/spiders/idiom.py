import re
import os
import scrapy
import sqlite3


class IdiomSpider2(scrapy.Spider):
    name = 'idiom2'

    def __init__(self):
        db_file = os.path.abspath(__file__ + "/../../../idiom.db")
        self.conn = sqlite3.connect(db_file)
        self.chengyus = set()

    def start_requests(self):
        print('start_requests')
        cursor = self.conn.cursor()
        cursor.execute('SELECT chengyu,pinyin,jieshi FROM idiom')
        for (chengyu, pinyin, jieshi) in cursor:
            if (not pinyin) and (not jieshi):
                print(chengyu)
                self.chengyus.add(chengyu)
                yield scrapy.FormRequest("http://cy.5156edu.com/serach.php",
                                         formdata={'f_key': chengyu, 'f_type': 'chengyu'},
                                         encoding='gb18030',
                                         callback=self.on_search)

    def on_search(self, response):
        urlpattern = re.compile('^/?(?:html|page|more).+\.html$')
        for href in response.css('a::attr(href)'):
            url = href.extract()
            if urlpattern.match(url):
                yield response.follow(href, self.parse)
        # here you would extract links to follow and return Requests for
        # each of them, with another callback
        # print(response)
        pass

    def parse(self, response):
        idiom = {}
        table3 = response.xpath('//table[@id="table3"]')
        title = table3.xpath('.//tr/td/font/b/text()')
        if title:
            idiom['成语'] = title.extract_first().strip()
        else:
            return
        if idiom['成语'] not in self.chengyus:
            return
        idiom['拼音'] = table3.xpath('.//tr/td/font/text()').extract_first()
        members = ['近义词', '反义词', '用法', '解释', '出处', '例子', '谒后语', '谜语', '成语故事']
        for member in members:
            path = './/tr/td/b[text()="{0}："]/following::td[1]/text()'.format(member)
            item = table3.xpath(path)
            if item:
                idiom[member] = item.extract_first().strip()
        update_sql = '''
                        update idiom set pinyin=?,jinyi=?,fanyi=?,
                        yongfa=?,jieshi=?,chuchu=?,lizi=?,xiehouyu=?,miyu=?,
                        gushi=? where chengyu="{0}"
                     '''.format(idiom['成语'])
        cursor = self.conn.cursor()
        cursor.execute(update_sql,(idiom.get('拼音', ''),
                                   idiom.get('近义词', ''),
                                   idiom.get('反义词', ''),
                                   idiom.get('用法', ''),
                                   idiom.get('解释', ''),
                                   idiom.get('出处', ''),
                                   idiom.get('例子', ''),
                                   idiom.get('谒后语', ''),
                                   idiom.get('谜语', ''),
                                   idiom.get('成语故事', ''),))
        self.conn.commit()

    def parse2(self, response):
        idiom = {}
        table3 = response.xpath('//table[@id="table3"]')
        title = table3.xpath('.//tr/td/font/b/text()')
        if title:
            idiom['成语'] = title.extract_first().strip()
        else:
            return
        if idiom['成语'] not in self.chengyus:
            return
        members = ['拼音', '简拼', '近义词', '反义词', '用法', '解释', '出处', '例子', '谒后语', '谜语', '成语故事']
        for member in members:
            path = './/tr/td[text()="{0}："]/following-sibling::td/text()'.format(member)
            item = table3.xpath(path)
            if item:
                idiom[member] = item.extract_first().strip()
            else :
                print('failed:',member)

        update_sql = '''
                        update idiom set pinyin=?,jianpin=?,jinyi=?,fanyi=?,
                        yongfa=?,jieshi=?,chuchu=?,lizi=?,xiehouyu=?,miyu=?,
                        gushi=? where chengyu="{0}"
                     '''.format(idiom['成语'])
        cursor = self.conn.cursor()
        cursor.execute(update_sql,(idiom.get('拼音', ''),
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
        self.conn.commit()
        # UPDATE
        # Person
        # SET
        # Address = 'Zhongshan 23', City = 'Nanjing'
        # WHERE
        # LastName = 'Wilson'
        # print(idiom)


class IdiomSpider(scrapy.Spider):
    name = 'idiom'

    start_urls = ['http://cy.5156edu.com']

    def __init__(self):
        file_path = os.path.abspath(__file__ + "/../idiom.json")
        self.out_file = open(file_path, 'w')

    def parse(self, response):
        # follow links to author pages
        # table1 = response.xpath('//table[@id="table1"]')
        #
        # table3 = response.xpath('//table[@id="table3"]')
        urlpattern = re.compile('^/?(?:html|page|more).+\.html$')
        for href in response.css('a::attr(href)'):
            url = href.extract()
            if urlpattern.match(url):
                yield response.follow(href, self.parse)

        idiom = {}
        table3 = response.xpath('//table[@id="table3"]')
        title = table3.xpath('.//tr/td/font/b/text()')
        if title:
            idiom['成语'] = title.extract_first().strip()
        else:
            return
        members = ['拼音', '简拼', '近义词', '反义词', '用法', '解释', '出处', '例子', '谒后语', '谜语', '成语故事']
        for member in members:
            path = './/tr/td[text()="{0}："]/following-sibling::td/text()'.format(member)
            item = table3.xpath(path)
            if item:
                idiom[member] = item.extract_first().strip()
        self.out_file.write(str(idiom) + '\r\n')
        # else:
        #     print('failed:', idiom)
        #     return
        # item = table3.xpath('.//tr/td[preceding-sibling::td[text()="简拼："]]/text()')
        # if item:
        #     idiom['简拼'] = item.extract_first()
        # else:
        #     return
        # print(idiom)
        # yield idiom
        # follow pagination links
        # for href in response.css('li.next a::attr(href)'):
        #     yield response.follow(href, self.parse)

    # def parse_idiom(self, response):
    #     def extract_with_css(query):
    #         return response.css(query).extract_first().strip()
    #
    #     yield {
    #         'name': extract_with_css('h3.author-title::text'),
    #         'birthdate': extract_with_css('.author-born-date::text'),
    #         'bio': extract_with_css('.author-description::text'),
    #     }
