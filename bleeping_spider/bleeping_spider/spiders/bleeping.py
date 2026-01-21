import re
import scrapy
from datetime import datetime, timedelta

class BleepingSpider(scrapy.Spider):
    name = "bleeping"
    allowed_domains = ["bleepingcomputer.com"]
    start_urls = ["https://www.bleepingcomputer.com/news/security/"]

    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.86 Safari/537.36",
        "ROBOTSTXT_OBEY": False,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 2,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "DOWNLOAD_DELAY": 3,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2
    }


    def parse(self, response):
        # Each article block
        articles = response.css("div.bc_latest_news_text")
        for article in articles:
            title = article.css("h4 a::text").get()
            url = article.css("h4 a::attr(href)").get()
            date = article.css("ul li.bc_news_date::text").get()
            author = article.css("ul li a.author::text").get()

            if not (title and url):
                continue  # skip articles with no title or URL

        
            title = title.strip()
            url = response.urljoin(url)
            date = date.strip() if date else "Unknown"
            from datetime import datetime, timedelta
            try:
                article_date = datetime.strptime(date, "%B %d, %Y")
                if article_date < datetime.now() - timedelta(days=1):
                    continue
            except:
                pass
            author = author.strip() if author else "Unknown"
            # Pass all extracted info to parse_article
            
            yield response.follow(
                url,
                callback=self.parse_article,
                cb_kwargs={"title": title, "date": date, "author": author, "url": url}
            )
            if article_date and article_date < datetime.now() - timedelta(days=1):
                return  # stop crawling older pages
            next_page = response.css('ul.cz-pagination li a[aria-label="Next Page"]::attr(href)').get()
            if next_page:
                yield response.follow(next_page, callback=self.parse)


    def parse_article(self, response, title, date, author, url):
        paragraphs = response.xpath('//div[@class="articleBody"]/*[not(descendant::figure)]')
        filtered_elements = [
            el for el in paragraphs
            if 'ia_ad' not in (el.attrib.get('class', '')) and 'cz-related-article-wrapp' not in (el.attrib.get('class', '')) 
            and el.root.tag not in ['figure', "style"] and el.xpath('string(.)').get().strip() != ""]

        texts = [el.xpath('string(.)').get().strip() for el in filtered_elements]
        body = "\n".join(texts)
        body = re.sub(r'(?<![.!?])\n', ' ', body)
        body = re.sub(r'[\t\r]', '', body)
        body = re.sub(r'\n+', '\n', body)
        body = body.replace(u'\xa0', u' ')

        yield {
            "title": title,
            "date": date,
            "author": author,
            "url": url,
            "body": body
        }

        


