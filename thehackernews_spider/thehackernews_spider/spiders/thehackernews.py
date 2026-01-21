import scrapy
import re
class TheHackerNewsSpider(scrapy.Spider):
    name = "thehackernews"
    allowed_domains = ["thehackernews.com"]
    start_urls = ["https://thehackernews.com/search/label/Vulnerability/"]

    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
        "ROBOTSTXT_OBEY": False,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 2,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2
    }


    def parse(self, response):
        # Each article block
        articles = response.css("div.body-post.clear")
        for article in articles:
            url = article.css("a.story-link::attr(href)").get()
            title = article.css("h2.home-title::text").get()
            date = article.css("span.h-datetime::text").get()

            if not (title and url):
                continue  # skip articles with no title or URL

        
            title = title.strip()
            url = response.urljoin(url)
            date = date.strip() if date else "Unknown"

            from datetime import datetime, timedelta
            try:
                article_date = datetime.strptime(date, "%B %d, %Y")
            except:
                try:
                    article_date = datetime.strptime(date, "%b %d, %Y")  # <-- FIX
                except:
                    article_date = None
            # Pass all extracted info to parse_article

            if article_date and article_date < datetime.now() - timedelta(days=1):
                return  # stop crawling older pages
            
            yield response.follow(
                url,
                callback=self.parse_article,
                cb_kwargs={"title": title, "date": date, "url": url}
            )

            next_page = response.css('a.blog-pager-older-link-mobile::attr(href)').get()
            if next_page:
                yield response.follow(next_page, callback=self.parse)



    def parse_article(self, response, title, date, url):
        from w3lib.html import remove_tags, replace_entities
        import re

        # Extract author
        authors = response.css('div.postmeta span.author::text').getall()
        author = authors[0].strip() if len(authors) > 1 else "Unknown"

        # === First Try: Extract paragraphs if available ===
        paragraphs = response.xpath(
            '//div[@id="articlebody"]//*[self::p or self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::h6][not(ancestor::div[contains(@class, "note-b")])]/descendant-or-self::text()').getall()

        #paragraphs = [p.get() for p in paragraphs if p.get().strip() != "#"] 

        if paragraphs and any(p.strip() for p in paragraphs):
            body = "\n".join([p.strip() for p in paragraphs if p.strip() and p.strip() != "#"])
        else:
            # === Fallback: Get entire content from #articlebody ===
            # Select all visible textual nodes (excluding .note-b div)
            raw_nodes = response.xpath(
                '//div[@id="articlebody"]//text()[not(ancestor-or-self::div[contains(@class, "note-b")])]'
            ).getall()

            # Clean, strip, and join
            lines = [replace_entities(text.strip()) for text in raw_nodes if text.strip()]
            body = "\n".join(lines)
            
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



