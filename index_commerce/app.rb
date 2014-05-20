require 'zillabyte'

app = Zillabyte.new("commerce_index")
  .source("select * from web_pages")
  .each{ |tuple|


    # Get the fields from your input data.
    url = tuple['url']
    html = tuple['html']
    
    # You care about three commerce technologies: Bluekai,
    # Gigya, and Scorecard Research.  However, you believe Bluekai
    # should count for more in your index.
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
      
    emit("url" => url, "score" => score)
  }
  .sink{
    name "has_hello"
    column "url", :string
  }

end