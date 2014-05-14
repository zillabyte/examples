require 'zillabyte'

app = Zillabyte.app "commerce_index"

input = app.source "select * from web_pages"

stream = input.each do |tuple|
  html = tuple['html']
  score = 0
  if html.include?('bluekai.com')
    score += 0.7
  end
  if html.include?('cdn.gigya.com/js/gigyaGAIntegration.js')
    score += 0.2
  end
  if html.include?('b.scorecardresearch.com/beacon.js')
    score += 0.1
  end

  emit {:url => tuple['url'], :score => score}
end
stream.sink do 
  name "commerce_index"
  column "url", :string
  column "score", :float
end