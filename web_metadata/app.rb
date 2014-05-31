require 'zillabyte'
require 'nokogiri'
require 'open-uri'

app = Zillabyte.app("web_metadata")
  .source("select * from web_pages")
  .each {|tuple|

    # Get the fields from your input data.
    url = tuple['url']
    html = tuple['html']

    # Extract meta keywords
    begin

      keywords = ""

      doc = Nokogiri::HTML(html)
      doc.xpath("//head//meta").each do |meta|

        # extract and clean keyword metadata content
        if meta["name"] == "keywords" && !meta["content"].nil?
          content = meta["content"].split(",")
          keywords = content.map {|w| w.strip().downcase!}.uniq().join(",")
        end
      end

      # emit any valid metadata
      if (keywords != "") 
          # emit the meta data
          emit :url => url, :keywords => keywords
      end
      
    rescue Exception
      puts "Malformed HTML, skipping"
      next
    end

  }
  .sink{
    name "web_keywords"
    column "url", :string
    column "keywords", :string
  }