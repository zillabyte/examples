require 'zillabyte'
require 'right_aws'

app = Zillabyte.app "tos"

input = app.source "select * from web_pages"

stream = input.each do
  prepare do
    $s3bucket = "tos_monitoring"
    s3 = RightAws::S3.new(accesskey, secretkey)
    $bucket1 = RightAws::S3::Bucket.create(s3, $s3bucket, true)
    $regex = Regexp.union('illegal','contraband','nefarious')
  end

  execute do |tuple|
    html = tuple['html']
    url = tuple['url']
    if html.include?("js.payment.processor.com")
      if not html.scan($regex).empty?

        # in order to name our .png files
        domain_regex = /http:\/\/w{3}?(\w*)/
        truncate_url = url.scan(domain_regex)


        if `casperjs screenshot.js #{url}` # use external casperjs file to take screenshot
          log "screenshot taken"

          key = RightAws::S3::Key.create($bucket1, "#{Date.today.to_s}/#{truncate_url[0][0]}.png") 

          x = File.read("match.png")
          key.put(x)

          emit{"URL" => url, "png_location"=> "s3://#{$s3bucket}/#{Date.today.to_s}/#{truncate_url[0][0]}.png"} # write url to the site

          log "file saved in s3"

          cleanup = `rm match.png`

          log "file deleted from cluster"
        else
          log "screenshot not taken"

        end
      end
    end
  end
end