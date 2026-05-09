#!/usr/bin/env python3
import urllib.request
import re
import html

url = "https://onlineradiobox.com/zm/chikuni/"
ua = "Mozilla/5.0 Chrome/122 Safari/537.36"

req = urllib.request.Request(url, headers={"User-Agent": ua})
page = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", errors="ignore")

page = re.sub(r"(?is)<script.*?</script>", " ", page)
page = re.sub(r"(?is)<style.*?</style>", " ", page)
page = re.sub(r"(?i)<br\s*/?>", "\n", page)
page = re.sub(r"(?i)</(div|li|tr|td|p|span|section)>", "\n", page)
page = re.sub(r"<[^>]+>", " ", page)
text = html.unescape(page)
text = re.sub(r"[ \t]+", " ", text)
text = re.sub(r"\n+", "\n", text)

low = text.lower()
pos = low.find("on the air")

print("FOUND_ON_THE_AIR_POSITION:", pos)

if pos >= 0:
    print(text[pos:pos+3000])
else:
    print(text[:3000])
