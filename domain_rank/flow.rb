require 'zillabyte'
require 'nokogiri'


# Create our 'flow', the DSL to help us orchestrate our app
flow = Zillabyte.new()


# A 'spout' is the beginning of a flow.  All data originates from the spout.
# Below, we will simply find which extrenal domains a given page may link to. 
stream = flow.spout do |h|

  h.matches "select * from web_pages"  # Let's consume the whole web. 
  h.emits [:source_domain, :target_domain]  # The data this spout emits.
  
  
  # This is called on every web page
  h.execute do |tuple, controller|
    
    base_url = tuple['url']
    html = tuple['html']
    doc = Nokogiri::HTML(html)
    
    doc.css('a').each do |link| 
      
      # What domain does this item link to? 
      target_uri = URI.join( base_url, link['href'])
      target_domain = target_uri.host.downcase
      
      # Emit this back to the flow.  This is important because it will allow
      # Zillabyte to parallelize the operation
      controller.emit :source_domain => source_domain, :target_domain => target_domain
      
    end
    
  end
end


# de-duplicate the stream.  i.e. throw out all tuples that have matching
# [source_domain, target_domain] pairs
stream.unique()


# Count the number of unique 'target_domain's.  By default, this will create a
# new field called 'count' and throw away all 'source_domain' values
stream.count :target_domain


# Final step, we need to sink the data into Zillabyte.  Sunk data is persistent
# and can be downloaded later. 
web_stream.sink do |h|
  h.name "domain_rank"
  h.column "domain", :string
  h.column "score", :integer
end
