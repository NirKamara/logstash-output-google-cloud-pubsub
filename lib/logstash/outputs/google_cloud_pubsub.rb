# encoding: utf-8
require 'gcloud'
require 'faraday'

module Faraday
  class Adapter
    class NetHttp < Faraday::Adapter
      def ssl_verify_mode(ssl)
        OpenSSL::SSL::VERIFY_NONE
      end
    end
  end
end

require "logstash/outputs/base"
require "logstash/namespace"

# Stream events to a Google Cloud PubSub topic
class LogStash::Outputs::GoogleCloudPubSub < LogStash::Outputs::Base
  config_name "google_cloud_pubsub"

  config :project

  # Path to JSON file containing the Service Account credentials (not needed when running inside GCE)
  config :keyfile

  # The name of the PubSub topic
  config :topic, :validate => :string, :required => true

  # Autocreate the topic if it doesn't exist
  config :autocreate_topic, :validate => :boolean, :default => true

  # The name of the project that the topic is in (if it's not the current project)
  config :topic_project, :validate => :string

  # Maximum number of messages to send in a single publish
  config :batch_size, :validate => :number, :default => 10
  
  # Number of Publish threads to be created
  config :publish_threads, :validate => :number, :default => 1
   
  # Max queue size, if this value is reach msgs are dropped (preventing out of memory)
  config :max_queue_size, :validate => :number, :default => 250000 

  config :output_format, :validate => [ "json", "plain" ], :default => "plain"

  default :codec, "plain"

  public
  def processMsg(threadNum)
	@logger.info("started publish thread ##{threadNum}")
	msgs = Array.new

        while true
			msgs << @queue.pop
			@logger.debug("thread: #{threadNum}, pulled msg from queue, msgs in thread: #{msgs.count}")
			if msgs.count >= @batch_size then
				begin
						retries ||=0
						@logger.info("thread: #{threadNum}, msgs to be flushed: #{msgs.count}, retry: #{retries}")
						@logger.info("thread: #{threadNum}, msgs in queue before publish: #{@queue.size}")
						@logger.info("thread: #{threadNum}, before sending: #{Time.now.strftime('%H:%M:%S.%L')}");
						# raise Exception.new("checking exception")
						@topic.publish do |t|
								msgs.each { |i| t.publish i }
								end
						@logger.info("thread: #{threadNum}, after sending: #{Time.now.strftime('%H:%M:%S.%L')}")
						msgs.clear
		
				rescue Exception => ex
					retries += 1
					@logger.error("Thread #{threadNum}, An error of type #{ex.class} happened, message is #{ex.message}")
					sleep [2 ** retries, 10].min
					retry
			end
		end
        end
  end

  def register
   begin
    @logger.info("Registering Google Cloud PubSub output", :project => @project, :keyfile => @keyfile, :topic => @topic)

    @pubsub = Gcloud.new(project=@project, keyfile=@keyfile).pubsub
    @topic = @pubsub.topic(@topic, { :autocreate => @autocreate_topic, :project => @topic_project })
    @queue = Queue.new
    @publish_threads.times do |i|
	@logger.debug("created thread id #{i}")
	@publisher = Thread.new do    
	processMsg(i)
	end 
    end

    # raise Exception.new("checking exception")
    raise "Topic #{@topic} not found" if not @topic
    @logger.debug("Topic: ", :topic => @topic)
  
   rescue Exception => ex
   	@logger.error("An error of type #{ex.class} happened, message is #{ex.message}")
   end
  end # def register

  public
  def receive(event)
	begin
		@logger.debug("event arrived...")
		if @queue.size <= @max_queue_size then
			@queue << event.to_json.to_s
			@logger.debug("event inserted into queue...")
		else
			logger.error("Queue max size reached. DROPPING MSG!!!")
		end
	rescue Exception => ex
		@logger.error("An error of type #{ex.class} happened, message is #{ex.message}")
	end
  end # def receive
end # class LogStash::Outputs::GCS
