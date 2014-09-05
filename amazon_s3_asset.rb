require 'aws/s3'
# Partnerpedia::AmazonS3Asset.new().copy_over_bucket("cisco-partnerpedia-production", "cisco-partnerpedia-staging")

module Partnerpedia
  class AmazonS3Asset
    include AWS::S3
    class_attribute :access_key_id, :secret_access_key

    def initialize
      puts "connecting..."
      s3 = {}
      if File.exists?("#{Rails.root}/config/s3.yml")
        s3_config = YAML.load_file("#{Rails.root}/config/s3.yml")
        self.access_key_id = s3_config[Rails.env]['access_key_id']
        self.secret_access_key = s3_config[Rails.env]['secret_access_key']
      else
        self.access_key_id = ENV['S3_KEY']
        self.secret_access_key = ENV['S3_SECRET']
      end

      AWS::S3::Base.establish_connection!(access_key_id: self.access_key_id, secret_access_key: self.secret_access_key)
    end

    def bucket_keys(bucket)
      marker_str = ""
      return_objs = []

      begin
        b = Bucket.objects(bucket, marker:marker_str)
        marker_str = b.last.key unless b.empty?
        puts "now marking #{marker_str}"
        return_objs.concat b.collect { |o| o.key }
      end while !b.empty?
      puts return_objs.size.to_s.color(:red)
      return return_objs
    end

    def copy_over_bucket(from_bucket, to_bucket)
      puts "Replacing #{to_bucket} with contents of #{from_bucket}"

      s3_handle = RightAws::S3Interface.new(self.access_key_id, self.secret_access_key)

      bucket_keys(from_bucket).each do |k|
        if !k.include?("logs/") && !k.include?("device_client_versions/")
          copy_between_buckets(s3_handle, from_bucket, to_bucket, k)
        end
      end
    end

    def copy_between_buckets(s3_handle, from_bucket, to_bucket, from_key, to_key = nil)
      if exists?(from_bucket, from_key)
        to_key = from_key if to_key.nil?
        puts "Copying #{from_bucket}.#{from_key} to #{to_bucket}.#{to_key}"
        begin
          s3_handle.copy(from_bucket, from_key, to_bucket, to_key)
          s3_handle.put_acl(to_bucket, to_key, s3_handle.get_acl(from_bucket, from_key)[:object])
        rescue RightAws::AwsError => e
          puts "FAILED Copying #{from_bucket}.#{from_key} to #{to_bucket}.#{to_key}"
        end
      else
        puts "#{from_bucket}.#{from_key} didn't exist"
        return nil
      end
    end

    def exists?(bucket,key)
      begin
        (res = S3Object.find key, bucket)
      rescue
        false
      end
    end

  end
end
