import zillabyte


# Score and emit links
def execute_score_pages(controller, tup):
  url = tup["url"]
  html = tup["html"]
  score = 0.0

  if "blukai" in html:
    score += 0.7
  if "cdn.gigya.com/js/gigyaGAIntegration.js" in html:
    score += 0.7
  if "b.scorecardresearch.com/beacon.js" in html:
    score += 0.7

  score += 1



  controller.emit( { "url" : url, "score" : score } )
  return


# Declare app
app = zillabyte.app(name="index_commerce")

# Retrieve all web pages
pages = app.source(matches="select * from web_pages")

# Score urls
scored_urls = pages.each(execute = execute_score_pages)

# Save pages to a relation
scored_urls.sink(name="commerce_index", columns=[{"url":"string"},{"score" : "float"}])






