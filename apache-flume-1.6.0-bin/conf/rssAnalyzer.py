import feedparser

fname = "rss.txt"
with open(fname) as f:
    content = f.readlines()
while True:
    for url in content:
        d = feedparser.parse(url)
        print d.entries[0].summary
