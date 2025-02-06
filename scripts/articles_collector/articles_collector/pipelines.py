# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy import signals
from scrapy.exporters import CsvItemExporter
from scrapy.exceptions import DropItem
from datetime import datetime, timedelta
import os

class DefaultValuesPipeline(object):
    def process_item(self, item, spider):
        item.setdefault('author', None)
        item.setdefault('title', None)
        item.setdefault('subtitle_preview', None)
        item.setdefault('collection', None)
        item.setdefault('read_time', None)
        item.setdefault('claps', None)
        item.setdefault('responses', None)
        item.setdefault('published_date', None)
        item.setdefault('scraped_date', None)
        return item

class CsvWriterPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        curr_path = os.path.dirname(os.path.realpath(__file__))
        scraped_data_path = os.path.abspath(os.path.join(curr_path, 'scraped_data'))
        if not os.path.exists(scraped_data_path):
            os.makedirs(scraped_data_path)
        current_date = datetime.now()
        previous_date = (current_date.replace(day=1) - timedelta(days=1))
        year = previous_date.year
        month = previous_date.month
        filename = f'raw_data_{year}_{month:02}.csv'
        write_path = os.path.join(scraped_data_path, filename)
        self.file = open(write_path, 'wb')
        self.exporter = CsvItemExporter(self.file)
        self.exporter.fields_to_export = ['author', 'title', 'subtitle_preview', 'collection', 'read_time', 'claps', 'responses', 'published_date', 'scraped_date']
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item