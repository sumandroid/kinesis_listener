namespace :kinesis do
  desc "starting up the kinesis listener....."
  task :listen => :environment do
    Rails.logger.tagged("Kinesis") { Rails.logger.info "Booting up the kinesis listener: #{Time.zone.now}" }
    kinesis_client = Aws::Kinesis::Client.new(region: ENV['aws-region'], access_key_id: ENV['aws_access_key_id'], secret_access_key: ENV['aws_secret_access_key'])
    kinesis_data_stream = ENV['kinesis_data_stream']
    consumer_arn = ENV['consumer_arn'] #if you dont have a consumer registered, skip this part and register a new consumer, or register a new consumer to stream.
    Rails.logger.tagged("Kinesis") { Rails.logger.info "checking up the availability of stream" }
    begin
      resp = get_stream_status(kinesis_data_stream)
      shards = resp.stream_description.shards
      stream_arn = resp.stream_description.stream_arn
      Rails.logger.tagged("Kinesis") { Rails.logger.info "checking the status of kinesis consumer"}
      consumer_resp = get_consumer_status(stream_arn, consumer_arn)
      if consumer_resp.consumer_description.consumer_status != 'ACTIVE'
        Rails.logger.tagged("Kinesis") { Rails.logger.info "consumer is not active, registering a new consumer"}
        consumer_reg_resp = register_stream_consumer(stream_arn)
        Rails.logger.tagged("Kinesis") { Rails.logger.info "checking the status of new consumer"}
        new_consumer_resp = get_consumer_status(stream_arn, consumer_reg_resp.consumer_description.consumer_arn)
        if new_consumer_resp.consumer_description.consumer_status != 'ACTIVE'
          Rails.logger.tagged("Kinesis") { Rails.logger.info "waiting for consumer to be active....."}
          sleep(10)
        end 
      end 
      Rails.logger.tagged("Kinesis") { Rails.logger.info "starting the listener......"}
      shards.each do |shard|
        Rails.logger.tagged("Kinesis") { Rails.logger.info "spawning thread for shard #{shard.shard_id}"}
        KinesisListenWorker.perform_async(shard.shard_id, kinesis_data_stream)
      end 
    rescue Exception => e
      Rails.logger.tagged("Kinesis") { e.message }
      exit
    end 
  end 
end
                                                                                                                     1,1           Top
def get_stream_status(kinesis_data_stream)
  resp = $kinesis_client.describe_stream({stream_name: kinesis_data_stream})
  Rails.logger.tagged("Kinesis") { Rails.logger.info "#{kinesis_data_stream} is #{resp.stream_description.stream_status}"}
  return resp
end

def get_consumer_status(stream_arn, consumer_arn)
  consumer_resp = $kinesis_client.describe_stream_consumer({
    stream_arn: stream_arn,
    consumer_arn: consumer_arn
  })
end

def register_stream_consumer(stream_arn)
  consumer_reg_resp = $kinesis_client.register_stream_consumer({
    stream_arn: stream_arn,
    consumer_name: "galactus_#{Time.zone.now.to_i}"
  })
  sleep(5)
  return consumer_reg_resp
end
                                                                                                                     57,1          Bot
