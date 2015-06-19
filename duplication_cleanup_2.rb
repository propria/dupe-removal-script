require 'digest/md5'
require "httparty"
require 'RMagick'
require 'iconv' unless String.method_defined?(:encode)
require 'csv'
input_file = open "possible_duplicates1.csv"
individual_images = input_file.lines

i = 0
CSV.open("output_csvs/checksums_" + i.to_s + ".csv", "wb") do |csv|

  puts Time.new.inspect

  failed = 0
  csv << ["user", "job", "image id", "file name", "checksum"]


  for n in 0..1
    individual_images.next #discarding the header of the csv
  end

  puts "Starting at line " + i.to_s

  individual_images.each do |image|

    #		if i == 2575 or i == 2575
    #			return
    #		end

    if String.method_defined?(:encode)
      image.encode!('UTF-16', 'UTF-8', :invalid => :replace, :replace => '')
      image.encode!('UTF-8', 'UTF-16')
    else
      ic = Iconv.new('UTF-8', 'UTF-8//IGNORE')
      file_contents = ic.iconv(file_contents)
    end

    image_data = image.split (",")
    user_id = image_data[8]
    job_id = image_data[5]

    if job_id.length == 0
      next
    end

    image_id = image_data[0]
    image_file_name = image_data[1]
    name = image_file_name.partition(".jpeg")[0]
    url = "http://s3.amazonaws.com/reo-prod/images/#{user_id}/#{job_id}/images/#{image_id}/#{name}_original.jpeg"
    file_url = "tmp/image_" + i.to_s + ".jpeg"

    begin
      tries = 0

      File.open(file_url, "w+") do |f|
        f.write HTTParty.get(url).parsed_response
        if f.size > 1024
          img1 = Magick::Image.read(file_url).first

          checksum = Digest::MD5.hexdigest img1.export_pixels.join

          csv << [user_id, job_id, image_id, image_file_name, checksum]

          i = i + 1

        else
          csv << [user_id, job_id, image_id, image_file_name, '']

          failed = failed + 1
          # puts "Failed Image : " + image_id + "\n  Size" + f.size.to_s

        end
        File.delete(file_url)
      end

    rescue HTTParty::Error => e
      puts 'HttParty::Error '+ e.message
      if tries < 3
        tries = tries + 1
        retry
      else
        failed = failed + 1
      end
    rescue StandardError => e
      puts 'StandardError '+ e.message
      failed = failed + 1
    end


    if i % 100 == 0
      puts Time.new.inspect
      puts i.to_s + " Images Processed"
      puts failed.to_s + " Images Failed"

    end

  end

  puts "--- All Done ---"
  puts i.to_s + " Images Processed"
  puts failed.to_s + " Images Failed"

end
