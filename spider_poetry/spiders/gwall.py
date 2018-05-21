import re
import os
from scrapy.selector import Selector
import scrapy
import sqlite3
import json
from scrapy.http import HtmlResponse
import requests

#爬古诗所有数据
class gwallSpider(scrapy.Spider):
    name = 'gwall'

    start_urls = ['https://so.gushiwen.org/gushi/tangshi.aspx']
    def __init__(self):
        db_file = os.path.abspath(__file__ + "/../../../guwen_all.db")
        self.conn = sqlite3.connect(db_file)
        c = self.conn.cursor()
        c.execute('''DROP TABLE guwen''')
        c.execute('''CREATE TABLE guwen
                     (title TEXT,zuozhe TEXT,chaodai TEXT,quanwen TEXT,yw TEXT,
                     yuny TEXT,zy TEXT,yiny TEXT,zs TEXT)''')

    def parse(self, response):
        urls = response.xpath('//div[@class="left"]//div[@class="sons"]//div[@class="typecont"]//span')
        for href in urls.css('a::attr(href)'):
            url = href.extract()
            yield response.follow(href, self.parse1)

    def parse1(self, response):
        rs = response.xpath('//div[@class="left"]//div[@class="sons"]')

        title = rs.xpath('//div[@class="cont"]//h1/text()').extract_first()
        print(title)
        quanwen = rs.xpath('//div[@class="cont"]//div[@class="contson"]')
        quanwen1 = quanwen[0].xpath('./text()').extract()
        print(quanwen1)

        qw = ""
        for item in quanwen1:
            if len(item) < 5:
                continue
            qw += item.strip()
            qw += '\r\n'

        if len(qw) < 12:
            quanwen = response.xpath('//div[@class="main3"]/div[@class="left"]/div[@class="sons"]/div[@class="cont"]/div[@class="contson"]').extract()[0]
            temp = re.sub(r'<p>', '', quanwen)
            temp = re.sub(r'</p>', '', temp)
            temp = re.sub(r'</div>', '', temp)
            temp = re.sub(r'(<div\s+class=\s*\".*?\">)', '', temp)
            qw1 = re.split('\<br>|\n', temp)
            for item in qw1:
                if len(item) < 5:
                    continue
                qw += item.strip()
                qw += '\r\n'
            # quanwen2 = quanwen[0].xpath('/p')
            # quanwen1 = quanwen2.xpath('./text()').extract()
            #quanwen1 = quanwen[0].xpath('/p/./text()').extract()
            # for item in quanwen1:
            #     qw += item.strip()
            #qw = '\r\n'.join(temp)
        print(qw)
        cd_zz = rs.xpath('//div[@class="cont"]//p//a/text()').extract()
        cd = str(cd_zz[0])
        zz = str(cd_zz[1])

        rs_all  = rs.xpath('//div[@class="contyishang"]')
        rs_h = rs_all.css('a::attr(href)')
        yw=""
        yuny=""
        zy=""
        yiny=""
        zs=""

        byw=True

        for href in rs_h:
            hreftxt = href.extract()
            if ("javascript:PlayFanyi" in hreftxt) and byw:
                print(hreftxt)
                byw=False
                yw_num = re.sub("\D", "", hreftxt)
                print(str(yw_num))
                yw_url = "https://so.gushiwen.org/fanyi_" + str(yw_num) +".aspx"
                html_requests = requests.get(yw_url).text.encode('utf-8')
                html_response = HtmlResponse(url=yw_url, body=html_requests, headers={'Connection': 'close'})
                rs = html_response.xpath(
                    '//div[@class="main3"]//div[@class="left"]//div[@class="contyishang"]/p[not(@style or contains(text(),"参考资料："))]').extract()
                for temp1 in rs:
                    temp = re.sub(r'<p>', '', temp1)
                    temp = re.sub(r'</p>', '', temp)
                    temp = re.sub(r'<strong>', '', temp)
                    temp = re.sub(r'</strong>', '', temp)
                    temp = re.sub(r'<a>', '', temp)
                    temp = re.sub(r'</a>', '', temp)
                    temp = re.sub(r'\u3000', '', temp)
                    temp = re.sub(r'(<a\s+href=\s*\".*?\">)', '', temp)
                    yw1 = re.split('\<br>', temp)
                    if yw1[0] == "译文":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        yw +=yw2
                    if yw1[0] == "韵译":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        yuny +=yw2
                    if yw1[0] == "直译":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        zy +=yw2
                    if yw1[0] == "音译":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        yiny +=yw2
                    if yw1[0] == "注释":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        zs +=yw2

                # temp = re.sub(r'<p>', '', rs[1])
                # temp = re.sub(r'</p>', '', temp)
                # temp = re.sub(r'<strong>', '', temp)
                # temp = re.sub(r'</strong>', '', temp)
                # temp = re.sub(r'<a>', '', temp)
                # temp = re.sub(r'</a>', '', temp)
                # temp = re.sub(r'\u3000', '', temp)
                # temp = re.sub(r'(<a\s+href=\s*\".*?\">)', '', temp)
                # zz1 = re.split('\<br>', temp)
                # zz2 =  '\r\n'.join(zz1)
                # print(zz1)
                #yield response.follow(yw_url, self.ywparse)



        c1 = self.conn.cursor()
        c1.execute("INSERT INTO guwen VALUES (?,?,?,?,?,?,?,?,?)",(title,zz,cd,qw,yw,yuny,zy,yiny,zs))

        self.conn.commit()

#名句
class MjgwallSpider(scrapy.Spider):
    name = 'mjgwall'

    #start_urls = ['https://so.gushiwen.org/mingju/Default.aspx?p=1&c=&t=']

    def __init__(self):
        db_file = os.path.abspath(__file__ + "/../../../guwen_all.db")
        self.conn = sqlite3.connect(db_file)
        c = self.conn.cursor()
        c.execute('''DROP TABLE guwen''')
        c.execute('''CREATE TABLE guwen
                     (title TEXT,zuozhe TEXT,chaodai TEXT,quanwen TEXT,yw TEXT,
                     yuny TEXT,zy TEXT,yiny TEXT,zs TEXT)''')
    def start_requests(self):
        n = 1
        while n <= 200:
            yield scrapy.FormRequest("https://so.gushiwen.org/mingju/Default.aspx?p={0}&c=&t=".format(n),
                                     callback=self.mingju_parse)
            n += 1

    def mingju_parse(self,response):
        songs2 = response.xpath('//div[@class="left"]//div[@class="sons"]')
        songs2 = songs2.css('a::attr(href)')
        count = 1
        while count < 100:
            url = songs2[count]
            count +=2
            yield response.follow(url, self.parse1)

    def parse1(self, response):
        rs = response.xpath('//div[@class="left"]//div[@class="sons"]')

        title = rs.xpath('//div[@class="cont"]//h1/text()').extract_first()
        print(title)
        quanwen = rs.xpath('//div[@class="cont"]//div[@class="contson"]')
        quanwen1 = quanwen[0].xpath('./text()').extract()
        print(quanwen1)

        qw = ""
        for item in quanwen1:
            if len(item) < 5:
                continue
            qw += item.strip()
            qw += '\r\n'

        if len(qw) < 12:
            quanwen = response.xpath('//div[@class="main3"]/div[@class="left"]/div[@class="sons"]/div[@class="cont"]/div[@class="contson"]').extract()[0]
            temp = re.sub(r'<p>', '', quanwen)
            temp = re.sub(r'</p>', '', temp)
            temp = re.sub(r'</div>', '', temp)
            temp = re.sub(r'(<div\s+class=\s*\".*?\">)', '', temp)
            qw1 = re.split('\<br>|\n', temp)
            for item in qw1:
                if len(item) < 5:
                    continue
                qw += item.strip()
                qw += '\r\n'
            # quanwen2 = quanwen[0].xpath('/p')
            # quanwen1 = quanwen2.xpath('./text()').extract()
            #quanwen1 = quanwen[0].xpath('/p/./text()').extract()
            # for item in quanwen1:
            #     qw += item.strip()
            #qw = '\r\n'.join(temp)
        print(qw)
        cd_zz = rs.xpath('//div[@class="cont"]//p//a/text()').extract()
        cd = str(cd_zz[0])
        zz = str(cd_zz[1])

        rs_all  = rs.xpath('//div[@class="contyishang"]')
        rs_h = rs_all.css('a::attr(href)')
        yw=""
        yuny=""
        zy=""
        yiny=""
        zs=""

        byw=True

        for href in rs_h:
            hreftxt = href.extract()
            if ("javascript:PlayFanyi" in hreftxt) and byw:
                print(hreftxt)
                byw=False
                yw_num = re.sub("\D", "", hreftxt)
                print(str(yw_num))
                yw_url = "https://so.gushiwen.org/fanyi_" + str(yw_num) +".aspx"
                html_requests = requests.get(yw_url).text.encode('utf-8')
                html_response = HtmlResponse(url=yw_url, body=html_requests, headers={'Connection': 'close'})
                rs = html_response.xpath(
                    '//div[@class="main3"]//div[@class="left"]//div[@class="contyishang"]/p[not(@style or contains(text(),"参考资料："))]').extract()
                for temp1 in rs:
                    temp = re.sub(r'<p>', '', temp1)
                    temp = re.sub(r'</p>', '', temp)
                    temp = re.sub(r'<strong>', '', temp)
                    temp = re.sub(r'</strong>', '', temp)
                    temp = re.sub(r'<a>', '', temp)
                    temp = re.sub(r'</a>', '', temp)
                    temp = re.sub(r'\u3000', '', temp)
                    temp = re.sub(r'(<a\s+href=\s*\".*?\">)', '', temp)
                    yw1 = re.split('\<br>', temp)
                    if yw1[0] == "译文":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        yw +=yw2
                    if yw1[0] == "韵译":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        yuny +=yw2
                    if yw1[0] == "直译":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        zy +=yw2
                    if yw1[0] == "音译":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        yiny +=yw2
                    if yw1[0] == "注释":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        zs +=yw2

        c1 = self.conn.cursor()
        c1.execute("INSERT INTO guwen VALUES (?,?,?,?,?,?,?,?,?)",(title,zz,cd,qw,yw,yuny,zy,yiny,zs))

        self.conn.commit()
#名句end

#名句
class Mjgwall1Spider(scrapy.Spider):
    name = 'mjgwall1'

    #start_urls = ['https://so.gushiwen.org/mingju/Default.aspx?p=1&c=&t=']

    def __init__(self):
        db_file = os.path.abspath(__file__ + "/../../../guwen_all.db")
        self.conn = sqlite3.connect(db_file)
        self.c = self.conn.cursor()

    def start_requests(self):
        rows = self.c.execute('select id,title from guwen WHERE yw="" and yuny="" and zy="" and yiny = "" and zs = ""')
        for row in rows:
            title = str(row[1])
            yield scrapy.FormRequest("https://so.gushiwen.org/search.aspx?value={0}".format(title),
                                     callback=self.mingju_parse)


    def mingju_parse(self,response):
        songs2 = response.xpath('//div[@class="left"]//div[@class="sons"]')
        songs2 = songs2.css('a::attr(href)')
        count = 1
        while count < 100:
            url = songs2[count]
            count +=2
            yield response.follow(url, self.parse1)

    def parse1(self, response):
        rs = response.xpath('//div[@class="left"]//div[@class="sons"]')

        title = rs.xpath('//div[@class="cont"]//h1/text()').extract_first()
        print(title)
        quanwen = rs.xpath('//div[@class="cont"]//div[@class="contson"]')
        quanwen1 = quanwen[0].xpath('./text()').extract()
        print(quanwen1)

        qw = ""
        for item in quanwen1:
            if len(item) < 5:
                continue
            qw += item.strip()
            qw += '\r\n'

        if len(qw) < 12:
            quanwen = response.xpath('//div[@class="main3"]/div[@class="left"]/div[@class="sons"]/div[@class="cont"]/div[@class="contson"]').extract()[0]
            temp = re.sub(r'<p>', '', quanwen)
            temp = re.sub(r'</p>', '', temp)
            temp = re.sub(r'</div>', '', temp)
            temp = re.sub(r'(<div\s+class=\s*\".*?\">)', '', temp)
            qw1 = re.split('\<br>|\n', temp)
            for item in qw1:
                if len(item) < 5:
                    continue
                qw += item.strip()
                qw += '\r\n'
            # quanwen2 = quanwen[0].xpath('/p')
            # quanwen1 = quanwen2.xpath('./text()').extract()
            #quanwen1 = quanwen[0].xpath('/p/./text()').extract()
            # for item in quanwen1:
            #     qw += item.strip()
            #qw = '\r\n'.join(temp)
        print(qw)
        cd_zz = rs.xpath('//div[@class="cont"]//p//a/text()').extract()
        cd = str(cd_zz[0])
        zz = str(cd_zz[1])

        rs_all  = rs.xpath('//div[@class="contyishang"]')
        rs_h = rs_all.css('a::attr(href)')
        yw=""
        yuny=""
        zy=""
        yiny=""
        zs=""

        byw=True

        for href in rs_h:
            hreftxt = href.extract()
            if ("javascript:PlayFanyi" in hreftxt) and byw:
                print(hreftxt)
                byw=False
                yw_num = re.sub("\D", "", hreftxt)
                print(str(yw_num))
                yw_url = "https://so.gushiwen.org/fanyi_" + str(yw_num) +".aspx"
                html_requests = requests.get(yw_url).text.encode('utf-8')
                html_response = HtmlResponse(url=yw_url, body=html_requests, headers={'Connection': 'close'})
                rs = html_response.xpath(
                    '//div[@class="main3"]//div[@class="left"]//div[@class="contyishang"]/p[not(@style or contains(text(),"参考资料："))]').extract()
                for temp1 in rs:
                    temp = re.sub(r'<p>', '', temp1)
                    temp = re.sub(r'</p>', '', temp)
                    temp = re.sub(r'<strong>', '', temp)
                    temp = re.sub(r'</strong>', '', temp)
                    temp = re.sub(r'<a>', '', temp)
                    temp = re.sub(r'</a>', '', temp)
                    temp = re.sub(r'\u3000', '', temp)
                    temp = re.sub(r'(<a\s+href=\s*\".*?\">)', '', temp)
                    yw1 = re.split('\<br>', temp)
                    if yw1[0] == "译文":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        yw +=yw2
                    if yw1[0] == "韵译":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        yuny +=yw2
                    if yw1[0] == "直译":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        zy +=yw2
                    if yw1[0] == "音译":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        yiny +=yw2
                    if yw1[0] == "注释":
                        del yw1[0]
                        yw2 = '\r\n'.join(yw1)
                        zs +=yw2

        c1 = self.conn.cursor()
        c1.execute("INSERT INTO guwen VALUES (?,?,?,?,?,?,?,?,?)",(title,zz,cd,qw,yw,yuny,zy,yiny,zs))

        self.conn.commit()
#名句end