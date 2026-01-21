from datetime import date, datetime
import scrapy
import re

class SecurityWeek(scrapy.Spider):
    name = "securityweek"
    allowed_domains = ["securityweek.com"]
    start_urls = ["https://www.securityweek.com/category/vulnerabilities/"]

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
        articles = response.css("article.zox-art-wrap")
        for article in articles:
            url = article.css("div.zox-art-title a::attr(href)").get()
            title = article.css("h2.zox-s-title2::text").get()
            author = article.css("span.zox-byline-name a::text").get()

            if not (title and url):
                continue  # skip articles with no title or URL

        
            title = title.strip()
            url = response.urljoin(url)
            author = author.strip() if author else "Unknown"
            # Pass all extracted info to parse_article
            
            yield response.follow(
                url,
                callback=self.parse_article,
                cb_kwargs={"title": title, "author": author, "url": url}
            )
        
        next_page_url = response.xpath('//div[@class="pagination"]/a[contains(text(), "Next")]/@href').get()
        if next_page_url:
            yield response.follow(next_page_url, callback=self.parse)




    def parse_article(self, response, title, author, url):
        # Extract the correct author (2nd .author span)
        date = response.css('time.post-date.updated::text').get()
        if date:
            date = date.strip()
        else: "Unknown"

            # === NEW CODE: 30-DAY FILTER ===
# Clean: extract only "Month DD, YYYY"
        from datetime import datetime, timedelta
        import re

        clean_date = re.match(r"^[A-Za-z]+\s+\d{1,2},\s+\d{4}", date)
        if clean_date:
            clean_date = clean_date.group(0)
        else:
            clean_date = date  # fallback if unexpected format

        # Parse date
        try:
            article_date = datetime.strptime(clean_date, "%B %d, %Y")
        except:
            try:
                article_date = datetime.strptime(clean_date, "%b %d, %Y")
            except:
                article_date = None

        # STOP when article older than 30 days
        if article_date and article_date < datetime.now() - timedelta(days=1):
            self.crawler.engine.close_spider(self, reason="older_than_threshold_days")
            return

        excerpt = response.xpath ('//span[contains(@class, "zox-post-excerpt")]/descendant-or-self::text()').getall()

        body = "\n".join([p.strip() for p in excerpt if p.strip()])
        body += "\n"
        # Extract paragraphs from #articlebody while excluding promotional/footer text in .note-b
        paragraphs = response.xpath(
            '//div[contains(@class, "zox-post-body")]//p['
            'not(ancestor::div[contains(@class, "zox-post-ad-wrap")]) and '
            'not(ancestor::div[contains(@class, "zox-author-box-wrap")]) and '
            'not(starts-with(normalize-space(string()), "Related:"))'
            ']/descendant-or-self::text()'
        ).getall()

        body += "\n".join([p.strip() for p in paragraphs if p.strip()])

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


