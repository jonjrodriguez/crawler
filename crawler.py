#!/usr/bin/env python

import argparse, os, robotparser, urllib2, sys
from heapq import heappush, heappop
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup

class Crawler(object):
    def __init__(self, docsPath, debug):
        self.pq = []
        self.seen = []
        self.pq_finder = {}
        self.docsPath = docsPath
        self.debug = debug
        self.rp = robotparser.RobotFileParser()

        if not os.path.exists(self.docsPath):
            os.makedirs(self.docsPath)

    def crawl(self, start_url, query, maxPages):
        print "Crawling for", maxPages, "pages relevant to", "'" + query + "'", "starting from", start_url

        self.addPage(start_url)
        while (self.pq or len(self.seen) > maxPages):
            url, html = self.getPage()
            if (html):
                if (len(self.seen) >= maxPages):
                    sys.exit()

                soup = BeautifulSoup(html, "html.parser")
                links = soup.find_all('a')

                for link in links:
                    href = link.get('href')
                    abs_url = urljoin(url, href)

                    if (self.seen.count(abs_url) == 0):
                        score = self.score(link, html, query)

                        if (not abs_url in self.pq_finder):
                            self.addPage(abs_url, score)
                        else:
                            if score:
                                self.addScore(abs_url, score)

    def addPage(self, url, score=0):

        name = url.rsplit('/', 1)[-1]

        if not name:
            name = "index.html"

        if not (name.endswith('.html') or name.endswith('.htm')):
            name = name + '.html'

        if (self.debug):
            print "Adding to queue:", url, "- Score =", score

        page = [-score, name, url]
        self.pq_finder[url] = page
        heappush(self.pq, page)

    def popPage(self):
        score, name, url = heappop(self.pq)
        del self.pq_finder[url]
        return -score, name, url

    def addScore(self, url, score):
        page = self.pq_finder.pop(url)

        if (self.debug):
            print "Adding", score, "to score of", url, "- New Score =", -(page[0] - score)

        page[0] = page[0] - score
        self.pq_finder[url] = page

    def getPage(self):
        score, name, url = self.popPage()

        if (self.debug):
                print
                print "Downloading:", url, "- Score =", score

        if (not self.robotSafe(url)):
            return url, False

        try:
            response = urllib2.urlopen(url)

            if (response.info().getheader('Content-Type').lower() == 'text/html; charset=utf-8'):
                html = response.read()
                self.writePage(url, name, html)
                return url, html
            else:
                return url, False
        except urllib2.HTTPError as e:
            print "Error:", e.code, ":", e.reason
            return url, False

    def writePage(self, url, name, html):
        self.seen.append(url)

        with open(os.path.join(self.docsPath, name), 'w') as f:
            f.write(html)

        if (self.debug):
            print "Received:", url

    def robotSafe(self, url):
        parsedUrl = urlparse(url)
        robotUrl = parsedUrl.scheme + "://" + parsedUrl.netloc + "/robots.txt"

        self.rp.set_url(robotUrl)

        self.rp.read()

        return self.rp.can_fetch("*", url)

    def score(self, link, html, query):
        if (not query):
            return 0

        words = query.lower().split()
        link_text = link.get_text().lower().strip()
        link_href = link.get('href').lower()
        
        count = 0
        for word in words:
            if word in link_text:
                count += 1
        
        if count:    
            return count * 50

        if (any(word in link_href for word in words)):
            return 40
        
        withinLink, text = self.cleanHtml(html.lower(), str(link).lower())

        u = 0
        v = 0
        for word in words:
            if word in withinLink:
                u += 1

            if word in text:
                v += 1

        return (4 * u) + (v - u) 

    def cleanHtml(self, html, link):
        remove_chars = dict.fromkeys(map(ord, ',.()"'), None)

        before, partition, after = html.partition(str(link))

        before = " ".join(BeautifulSoup(before, 'html.parser').get_text().translate(remove_chars).split()).split()

        after = " ".join(BeautifulSoup(after, 'html.parser').get_text().translate(remove_chars).split()).split()

        return before[-5:] + after[:5], before + after

def main():
    parser = argparse.ArgumentParser(description='Crawl a webpage for related links')

    parser.add_argument('-u', metavar="URL", required=True, help='The URL from which to start the crawl')
    parser.add_argument('-q', metavar="QUERY", default="", help='The query to find related pages')
    parser.add_argument('-docs', metavar="DOCS_PATH", default="docs", help='The path where the documents are stored (default: docs)')
    parser.add_argument('-m', metavar="MAX_PAGES", default=50, type=int, help='The maximum number of pages to download (default: 50)')
    parser.add_argument('-t', '--trace', dest="trace", action='store_true', help='Include to generate a trace')

    args = parser.parse_args()

    crawler = Crawler(args.docs, args.trace)

    crawler.crawl(args.u, args.q, args.m)

if __name__ == '__main__':
    main()