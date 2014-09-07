require 'zillabyte'

# For URI parsing
require 'uri'
require 'open-uri'

# For XML parsing
require 'nokogiri'
require 'equivalent-xml'

# For RDF
require 'rdf/rdfa'
require 'sparql'

# We reuse two values several times over the course of this app: the base address of the RDF schema and
# the list of attributes that define our final schema. So we define them here.
SCHEMA = RDF::Vocabulary.new "http://schema.org/"

SCHEMA_ATTRIBUTES = ["address_locality", "hiring_organization", "title", "description", "name"]

app = Zillabyte.app "zillabyte_indeed"

# We start by outputting the URLs from which we wish to extract information.
# This has been simplified considerably to only deal one page and one search term.
stream = app.source do
  next_tuple do
    emit "url" => "http://www.indeed.com/jobs/?q=web+crawling"
  end
end

# Next, we process each URL by interpreting it as an RDF graph.
stream = stream.each do |tuple|
  name "each_job_posting"

  execute do |tuple|
    # We wrap the entire execute in a begin-rescue block, so that for unexpected exceptions we do not break our app
    # completely. Note that one should *NOT* catch 'Exception', ever -- this breaks signal handling and prevents
    # clean shutdown of an app.
    begin
      # Using the third party library, we first load the graph...
      url = tup["url"]
      rdf_graph = RDF::Graph.load url

      # Then construct a query for each predicate in the document...
      job_posting_query = RDF::Query.new job_posting: {
        RDF.type => SCHEMA.JobPosting,
        SCHEMA.hiringOrganization => :hiring_organization,
        SCHEMA.jobLocation => :job_location,
        SCHEMA.title => :title,
        SCHEMA.description => :description,
        SCHEMA.name => :name,
      }

      job_location_query = RDF::Query.new job_location: {
        RDF.type => SCHEMA.Place,
        SCHEMA.address => :address,
      }

      job_postal_address_query = RDF::Query.new :address => {
        RDF.type => SCHEMA.Postaladdress,
        SCHEMA.addressLocality => :address_locality,
      }

      # Here, we join the three queries using Ruby's SPARQL gem (SPARQL is a language that is based on relational
      # algebra, like SQL, but also features a number of important differences), and then execute the query to
      # arrive at a solution:
      solution_set = SPARQL::Algebra::Operator::Join.new(
        SPARQL::Algebra::Operator::Join.new(job_location_query, job_posting_query),
        job_postal_address_query
      ).execute rdf_graph

      # To close out the execute, we iterate over the solution set and emit a tuple for each record.
      solution_set.each do |solution|
        emit solution.bindings.select { |attribute, *| SCHEMA_ATTRIBUTES.include? attribute.to_s }
      end
    rescue => e
      # Print to standard error--this is more semantically correct and will show up during execution of `zillabyte apps:test`.
      STDERR.puts "Error:\n" + e.message + "\n" + e.backtrace.join("\n")
    end
  end
end

# The last step, as always, is to sink our newly extracted information to the database for later analysis.
stream.sink do
  name "indeed_job_posting"
  column "address_locality", :string
  column "hiring_organization", :string
  column "title", :string
  column "description", :string
  column "name", :string
end
