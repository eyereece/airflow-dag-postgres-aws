from datetime import datetime, timedelta
from scrapy.spiders import Spider
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst

import sys
import os
curr_path = os.path.dirname(os.path.realpath(__file__))
base_path = os.path.abspath(os.path.join(curr_path, os.pardir))
sys.path.append(base_path)

from items import ArticlesCollectorItem
from datetime import datetime, timedelta

class ArticleSpider(Spider):
    name = "first_spider"
    start_urls = [#'https://blog.det.life/archive',
                # 'https://medium.com/javarevisited/archive/',
                # 'https://towardsdatascience.com/archive',
                # 'https://pub.towardsai.net/archive',
                'https://levelup.gitconnected.com/archive',
    ]
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 5,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'pipelines.DefaultValuesPipeline': 100,
            'pipelines.CsvWriterPipeline': 200
        }
    }

    def __init__(self, *args, **kwargs):
        super(ArticleSpider, self).__init__(*args, **kwargs)
        self.start_date, self.end_date = self.calculate_dates()


    def calculate_dates(self):
        # Get current date
        today = datetime.now()
        start_date = today.replace(day=1) - timedelta(days=1)
        start_date = start_date.replace(day=1) # Set to the first of last month

        # Calculate end date
        end_date = today.replace(day=1) - timedelta(days=1)
        return start_date, end_date
    
    def parse(self, response):
        year_pages = response.xpath('/html/body/div[1]/div[2]/div/div[3]/div[1]/div[1]/div/div[2]/*/a/@href').getall()
        year_to_search = self.start_date.year
        found_year = False
        for year_page in year_pages:
            if str(year_to_search) in year_page:
                found_year = True
                yield response.follow(year_page, callback=self.parse_months)
        if not found_year:
            self.logger.info(f'{year_to_search} year page not found.')

    def parse_months(self, response):
        month_pages = response.xpath('/html/body/div[1]/div[2]/div/div[3]/div[1]/div[1]/div/div[3]/div/a/@href').getall()
        month_to_search = self.start_date.month
        found_month = False
        for month_page in month_pages:
            if str(month_to_search) in month_page:
                found_month = True
                yield response.follow(month_page, callback=self.parse_days)
        if not found_month:
            self.logger.info(f'{month_to_search} not found')

    def parse_days(self, response):
        # parse all days present
        day_pages = response.xpath('/html/body/div[1]/div[2]/div/div[3]/div[1]/div[1]/div/div[4]/div/a/@href').getall()
        if len(day_pages) != 0:
            yield from response.follow_all(day_pages, callback=self.parse_articles)
        else:
            yield from self.parse_articles(response)

    def parse_articles(self, response):
        articles = response.xpath('/html/body/div[1]/div[2]/div/div[3]/div[1]/div[2]/*')
        for article_selector in articles:
            item = self.populate_item(article_selector, response.url)
            published_date = item.get('published_date')
            if isinstance(published_date, datetime):
                if self.start_date <= published_date <= self.end_date:
                    yield item
            else:
                self.logger.warning(f'Published date is not a datetime object: {published_date}')

    def populate_item(self, selector, url):
        item_loader = ItemLoader(item=ArticlesCollectorItem(), selector=selector)
        item_loader.default_output_processor = TakeFirst()
        item_loader.add_xpath('author', './/a[@data-action="show-user-card"]/text()')
        item_loader.add_xpath('title', './/*[contains(@class, "title")]/text()')
        item_loader.add_xpath('title', './/h3[contains(@class, "title")]/*/text()')
        item_loader.add_xpath('subtitle_preview', './/*[@name="previewSubtitle"]/text()')
        item_loader.add_xpath('collection', './/a[@data-action="show-collection-card"]/text()')
        item_loader.add_xpath('read_time', './/*[@class="readingTime"]/@title')
        item_loader.add_xpath('claps', './/button[@data-action="show-recommends"]/text()')
        item_loader.add_xpath('responses', './/a[@class="button button--chromeless u-baseColor--buttonNormal"]/text()')
        item_loader.add_xpath('published_date', './/time/text()')
        item_loader.add_value('scraped_date', datetime.now())
        return item_loader.load_item()

if __name__ == '__main__':
    from scrapy.crawler import CrawlerProcess
    process = CrawlerProcess()
    process.crawl(ArticleSpider)
    process.start()