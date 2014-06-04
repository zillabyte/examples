from bs4 import BeautifulSoup
import urllib2
import zillabyte

def open_url(url):
  try:
    page = urllib2.urlopen(url).read()
  except:
    page = None
  return page

def execute_find_links(controller, tup):
  domain = tup["domain"]
  if domain[0:4] == "www.":
    domain = domain[5:]

  url="http://"+tup["domain"]
  page=open_url(url)

  if(page != None):
    soup = BeautifulSoup(page)
    links = soup.findAll('a', href=True)
    same_domain_links = filter(lambda link: domain in str(link["href"]), links)

    for link in same_domain_links:
      controller.emit({"domain":tup["domain"], "url":link["href"]})
  return

def execute_crawl(controller, tup):
  url = tup["url"]
  page = open_url(url)

  if(page != None):
    controller.emit({"domain":tup["domain"], "url":url, "html":page})
  return

app = zillabyte.app(name="python_crawler")

stream = app.source(matches="select * from domains")
stream = stream.each(execute=execute_find_links)
stream = stream.each(execute=execute_crawl)
stream.sink(name="domain_pages", columns=[{"domain":"string"}, {"url":"string"}, {"html":"string"}])
