require 'zillabyte'
require 'nokogiri'


# Create our 'app', the DSL to help us orchestrate our app
app = Zillabyte.app("domain_rank")


# A 'source' is the beginning of a app.  All data originates from the source.
# Below, we will simply find which extrenal domains a given page may link to. 
stream = app.source "select * from web_pages"

links = stream.each do |tuple|  
  # This is called on every web page
  base_url = tuple['url']
  html = tuple['html']
  doc = Nokogiri::HTML(html)

  doc.css('a').each do |link| 
    # What domain does this item link to? 
    target_uri = URI.join( base_url, link['href'])
    target_domain = target_uri.host.downcase

    # Emit this to a stream.  This is important because it will allow
    # Zillabyte to parallelize the operation
    emit :source_domain => source_domain, :target_domain => target_domain
  end
end

# de-duplicate the stream.  i.e. throw out all tuples that have matching
# [source_domain, target_domain] pairs
unique_links = links.unique()


# Count the number of unique 'target_domain's.  By default, this will create a
# new field called 'count' and throw away all 'source_domain' values
web_stream = unique_links.count :target_domain


# Final step, we need to sink the data into Zillabyte.  Sunk data is persistent
# and can be downloaded later. 
web_stream.sink do |h|
  h.name "domain_rank"
  h.column "domain", :string
  h.column "score", :integer
end

