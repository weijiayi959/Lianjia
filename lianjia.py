import requests
from lxml import etree
import pymongo
from requests.exceptions import RequestException
import re
from bs4 import BeautifulSoup
import copy
from multiprocessing import Pool

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.info
table = db.info

def get_page(url):
    try:
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.75 Safari/537.36'}
        response = requests.get(url, headers=headers)
        response.encoding = response.apparent_encoding
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        return None


def parse_page(response):
    try:
        response1 = copy.copy(response)
        response2 = re.sub(r'<span\sclass="divide">/</span>', ',', response1)
        pattern = re.compile('<a\shref=.*?"region">.*?</a>.*?(\d.\d.).*?</div>', re.S)
        info = re.findall(pattern, response2)
        response = etree.HTML(response)
        title = response.xpath('//div[@class="title"]/a/text()')
        address = response.xpath('//div[@class="houseInfo"]/a/text()')
        info_one = response.xpath('//div[@class="houseInfo"]/text()[2]')
        info_two = response.xpath('//div[@class="houseInfo"]/text()[3]')
        info_three = response.xpath('//div[@class="houseInfo"]/text()[4]')
        info_four = response.xpath('//div[@class="houseInfo"]/text()[5]')
        position_one = response.xpath('//div[@class="positionInfo"]/text()[1]')
        position_two = response.xpath('//div[@class="positionInfo"]/text()[2]')
        position_three = response.xpath('//div[@class="positionInfo"]/a/text()')
        follow_info = response.xpath('//div[@class="followInfo"]/text()[1]')
        follow_action = response.xpath('//div[@class="followInfo"]/text()[2]')
        tag_subway = response.xpath('//div[@class="tag"]/span[@class="subway"]/text()')
        tag_taxfree = response.xpath('//div[@class="tag"]/span[@class="taxfree"]/text()')
        tag_haskey = response.xpath('//div[@class="tag"]/span[@class="haskey"]/text()')
        totalPrice = response.xpath('//div[@class="totalPrice"]/span/text()')
        unitPrice = response.xpath('//div[@class="unitPrice"]/span/text()')
        for item in range(len(title)):
            yield {
                'title': title[item],
                'address': address[item],
                'info':info[item],
                'info_one': info_one[item],
                'info_two': info_two[item],
                'info_three': info_three[item],
                'info_four': info_four[item],
                'position_one': position_one[item],
                'position_two': position_two[item],
                'position_three': position_three[item],
                'follow_info': follow_info[item],
                'follow_action': follow_action[item],
                'tag_subway': tag_subway[item],
                'tag_taxfree': tag_taxfree[item],
                'tag_haskey': tag_haskey[item],
                'totalPrice': totalPrice[item]+'w',
                'unitPrice': unitPrice[item],
            }
    except IndexError:
        pass


def save_to_mongodb(item):
    if table.insert(item):
        print('save to mongo!')
    count = table.find().count()
    print(count)


def main(i):
    url = 'https://bj.lianjia.com/ershoufang/pg{}/'.format(i)
    response = get_page(url)
    for item in parse_page(response):
        save_to_mongodb(item)


if __name__ == "__main__":
    pool = Pool()
    pool.map(main, [i for i in range(1, 101)])
