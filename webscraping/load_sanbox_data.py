import scrapy
from scrapy.crawler import CrawlerProcess
import json

class OpenSourceSpider(scrapy.Spider):
    name = 'opensource_spider'
    start_urls = ['https://news.ycombinator.com/'] # Using an open site (Hacker News)

    def parse(self, response):
        scraped_data = []
        
        # Extract structured data using CSS selectors
        for row in response.css('tr.athing'):
            item = {
                'title': row.css('.titleline > a::text').get(),
                'link': row.css('.titleline > a::attr(href)').get(),
                'rank': row.css('.rank::text').get()
            }
            if item['title']:
                scraped_data.append(item)
            yield item

        # Integration step: Persist the scraped data into a shared data store
        with open('shared_data.json', 'w') as f:
            json.dump(scraped_data, f, indent=4)

# Boilerplate to run the scraper via script
if __name__ == "__main__":
    process = CrawlerProcess(settings={"LOG_LEVEL": "DEBUG"})
    process.crawl(OpenSourceSpider)
    process.start()
    print("Scraping complete. Data saved to shared_data.json")
