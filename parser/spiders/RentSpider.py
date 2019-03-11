import datetime
import logging
import re

import scrapy
import pandas as pd

from olx.parser.utils import handlers


logging.basicConfig(level=logging.DEBUG, filename="olx/parser/log/spider.log")

class RentspiderSpider(scrapy.Spider):
    name = 'RentSpider'
    start_urls = ['https://www.olx.ua/nedvizhimost/posutochno-pochasovo/kiev/']
    premium_mapper = {}

    def parse(self, response):

        print("processing:", response.url)
        pages_numbers = response.xpath("/html/body/div[2]/div[4]/section/"
                                       "div[3]/div/div[5]/span[@class='item fleft']/a/@href").extract()
        pages_numbers = [i.split("=")[1] for i in pages_numbers]
        last_number = pages_numbers[-1]

        if last_number:
            last_number = int(last_number)
            pages_urls = [f"{RentspiderSpider.start_urls[0]}?page={i}" for i in range(1,last_number+1)]
            for page_url in pages_urls[::-1]:
                yield scrapy.Request(page_url, callback=self.parse_page)

    def parse_page(self, response):

        print("processing page:", response.url)

        thumbs = response.css(".thumb").extract()
        mapper = handlers.get_href_to_premium(thumbs)
        self.premium_mapper = {**self.premium_mapper,**mapper}
        try:
            details_links = response.css(".detailsLink ::attr(href)").extract()
            for detail_link in details_links:
                yield  scrapy.Request(detail_link, callback=self.parse_item)
        except Exception as e:
            print(e)



    def process_publication_info(self, response):
        try:
            publication_time = response.css('em ::text').extract()[0].strip()
            publication_id = response.css('em ::text').extract()[1].strip()
            publication_id = publication_id.split(":")[1]
        except IndexError:
            publication_time = response.css('em ::text').extract()[3].strip()
            publication_id = response.css('em ::text').extract()[4].strip()
            publication_id = publication_id.split(":")[1]

        pub_time = re.findall(r"\d\d:\d\d", publication_time)[0]
        pub_date = publication_time.split(",")[1]

        return pub_date,pub_time,publication_id


    def parse_item(self, response):

        price = response.css(".price-label ::text").extract()[1]
        name = response.css(".offer-titlebox ::text").extract()[1].strip()
        location = response.css(".show-map-link ::text").extract()[0]
        pub_time,pub_date,publication_id = self.process_publication_info(response)
        text = response.css("#textContent ::text").extract()
        processed_text = handlers.clean_text(text)

        images = response.css(".photo-glow > img ::attr(src)").extract()

        publication_id = publication_id.strip()
        tag_keys = response.css("tr > th ::text").extract()
        tag_text = response.css("tr > .value ::text").extract()
        tag_values = handlers.clean_group_tags(tag_text)
        tag_dict = {tag_keys[i]:tag_values[i] for i in range(len(tag_keys))}
        tag_dict = {key.replace(".","_"):value for key,value in tag_dict.items()}

        views = response.css(".pdingtop10 > strong ::text").extract()[0]
        is_premium = self.premium_mapper.get(response.url)
        coords = re.findall(r"[0-9][0-9]\.[0-9][0-9]+", response.text)
        coords = [i for i in coords if len(i) > 6]
        latitude = coords[0]
        longitude = coords[1]

        record = {
            "_id": publication_id,
            "url":response.url,
            "price":price,
            "publication_time":pub_time,
            "title": name,
            "location": {
                "district":location,
                "latitude":float(latitude),
                "longitude":float(longitude),
            },
            "text":processed_text,
            "images":images,
            "tags":tag_dict,
            "views_count":int(views),
            "premium": is_premium,
            "parse_time": datetime.datetime.now()
        }
        yield  record

        

