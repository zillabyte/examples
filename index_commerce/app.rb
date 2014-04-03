require 'zillabyte'

Zillabyte.simple_app do

  # Every Zillabyte app needs a name
  name "commerce_index"

  # Your function will have access
  # to two fields as input data: URL and HTML
  matches "select * from web_pages"

  # Emit a tuple that is two-columns wide and contains 
  # the attributes 'URL' and 'score', in the relation
  # named "commerce_index".
  emits   [
    ["commerce_index", [{"URL"=>:string}, {"score"=> :float}]]
  ]

  # This is the heart of your algorithm.  It's processed on every
  # web page.  This algorithm is run in parallel on possibly hundreds
  # of machines. 
  execute do |tuple|

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

    emit("commerce_index", "URL" => url, "score" => score)
  end

end
