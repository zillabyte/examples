require 'zillabyte'

app = Zillabyte.app("shopify")  

input = app.source "select * from web_pages"

stream = input.each do |tuple|
  html = tuple['html']
  url = tuple['url']
  # look for pages that are built on shopify.  
  if html.scan('myshopify.com') or html.scan('shopify.shop') or html.scan('shopify.theme') 

    # all san francisco zip codes begin with 941xx.
    # make a regular expression to find 5 digit numbers
    # that have a whitespace before it
    # and end in either a comma or a whitespace
    regex_for_zip = Regexp.new('\s(941[\d]{2})[,\s]')

    ary = html.scan(regex_for_zip)

    unless ary.empty?

      zip = ary[0]
      # emit a new table called shopify_in_sf,
      # with two columns, the URL and the zipcode
      emit :url => url, :zipcode => zip
    end

  end

end

stream.sink do 
  name "shopify_SF"
  column "url", :string
  column "zipcode", :integer
end
