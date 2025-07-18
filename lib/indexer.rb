require 'data_collector'
require 'lib/elastic'
require 'thread'
require_relative 'error'
require 'lib/indexer/solis'

if Dir.exist?('./config/rules')
  Dir.glob('./config/rules/*_rules.rb').each do |rule|
    puts "Loading rule #{rule}"
    require "#{rule}"
  end
else
  puts "No rules found"
  exit!
end

class Indexer
  attr_reader :index_name, :queue, :stats, :workers

  def initialize()
    @running = true
    @queue = Queue.new
    @config = DataCollector::ConfigFile[:services][:data_indexer]
    @elastic = setup_elastic
    @workers = []
    @stats = {'load' => {}, 'error' => {}}

    setup_index_workers(@config[:elastic][:workers])
  end

  def index(data)
    if data
      data = process(data)
      @stats['load'].merge!(DataCollector::Core.filter(data, '$[*].fiche.data._id').map{|m| m.split('/')[-2]}.tally){|k,o,n| o+n}
      result = @elastic.index.insert(data, 'fiche.data._id', false)
      if result.empty? || result['items'].size != data.size
        all_in_ids = DataCollector::Core.filter(data, '$[*].fiche.data._id').sort
        all_out_ids = result['items'].map{|m| m['index']['_id']}.sort rescue []
        missing = all_in_ids - all_out_ids
        if missing.size > 0
          raise Error::IndexError, missing.join(', ')
        end
      end
    end
  rescue StandardError => e
    errors = JSON.parse(e.message)
    if errors
      @stats['error'].merge!(errors.keys.map{|m| m.split('/')[-2]}.tally){|k,o,n| o+n}
    end
    raise Error::IndexError, e.message
  end

  def count
    @elastic.index.count
  end

  def recreate
    raise Error::IndexError, 'Please supply block' unless block_given?
    @index_alias = @config[:elastic][:index]
    @current_index_name = @elastic.alias.index(@index_alias).first
    @new_index_name = "#{@index_alias}_#{Time.now.to_i}"
    @elastic = Elastic.new(@new_index_name,
                           @config[:elastic][:mapping],
                           @config[:elastic][:host],
                           DataCollector::Core.logger)

    @elastic.index.create
    yield

    DataCollector::Core.log("Attaching alias to new index: #{@new_index_name}:#{@index_alias}")
    @elastic.alias.replace(@index_alias, @current_index_name, @new_index_name)
    DataCollector::Core.log("Removing old index: #{@current_index_name}")
    @elastic.index.delete(@current_index_name)
    DataCollector::Core.log("#{@elastic.index.count} records indexed")
  end

  def running?
    @running
  end

  def stop
    @running = false
    @workers.each do |worker|
      DataCollector::Core.log("Draining index worker #{worker[:name]}")
      worker.join(5) unless worker.stop?
    end
  end



  def process(metadata)
    return if metadata.empty? || metadata.nil? || !running?
    result = []
    rule_name = 'GENERIC_RULES'
    begin
      rule_name = "#{metadata.first['id'].split('/')[-2].classify.upcase}_RULES"
    rescue StandardError => e
      begin
        rule_name = "#{metadata.first['_id'].split('/')[-2].classify.upcase}_RULES"
      rescue StandardError => e
        rule_name = 'GENERIC_RULES'
      end
    ensure
      rule_name = 'GENERIC_RULES' if rule_name.nil?
    end

    begin
      raise "Rule not defined: #{rule_name}" unless Object.const_defined?(rule_name)
      rules = Object.const_get(rule_name)
    rescue StandardError => e
      DataCollector::Core.log("Available rules: #{Object.constants.grep(/RULES/).join(", ")}")
      rule_name = 'GENERIC_RULES'
      retry
    end

    output = DataCollector::Output.new
    metadata.each do |data|
      output.clear
      DataCollector::Core.rules.run(rules, data, output, { "_no_array_with_one_element" => true, solis: Solis::Options.instance })
      result << output.raw
    end
    result.each { |m| m.deep_stringify_keys! }
  rescue => e
    raise e, "Error processing #{rule_name} -> #{e.message}"
  end

  private
  def setup_elastic
    @index_alias = @config[:elastic][:index]
    @new_index_name = "#{@index_alias}_#{Time.now.to_i}"
    elastic = Elastic.new(@new_index_name,
                          @config[:elastic][:mapping],
                          @config[:elastic][:host],
                          DataCollector::Core.logger)

    @index_name = elastic.alias.index(@index_alias).first

    if @index_name.nil? || @index_name.empty?
      elastic.index.create
      elastic.alias.add(@index_alias, @new_index_name)
    end

    elastic = Elastic.new(@index_alias,
                          @config[:elastic][:mapping],
                          @config[:elastic][:host],
                          DataCollector::Core.logger)

    DataCollector::Core.log("Using elastic index: #{@index_name}:#{@index_alias}")
    elastic
  end

  def setup_index_workers2(number_of_workers)
    number_of_workers.times do |i|
      @workers << Thread.new do
        Thread.current[:name] = i.to_s
        DataCollector::Core.log("Starting index worker #{Thread.current[:name]}")
        data = []
        prev_clock = 0
        while @running
          begin
            Thread.current[:data] = data
            current_clock = Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i
            if current_clock.modulo(30) == 0 && prev_clock != current_clock
              DataCollector::Core.log("To be indexed: #{@queue.size}")
              prev_clock = current_clock
            end
            if @queue.size > 0
              begin
                raw_data = @queue.pop(true) #non blocking
                if raw_data.is_a?(Array)
                  data += raw_data
                else
                  DataCollector::Core.log("BAD data: #{raw_data.inspect}")
                end
              rescue ThreadError #empty queue
                sleep 0.2
              end
            else
              # DataCollector::Core.log("Indexer is waiting for data")
              sleep 10
            end

            if data.size > 100 || current_clock.modulo(30) == 0
              DataCollector::Core.log("SAVING - start")
              self.index(data) if data.size > 0
              data = []
              DataCollector::Core.log("SAVING - done")
            end
          rescue StandardError => e
            DataCollector::Core.log("Error indexing: #{e.message}")
            data = []
          end
        end
        self.index(data) if data.size > 0
        DataCollector::Core.log("Stopping index worker #{Thread.current[:name]}")
      end
    end
  rescue StandardError => e
    DataCollector::Core.log("Error indexing: #{e.message}")
  end

  def setup_index_workers(number_of_workers)
    number_of_workers.times do |i|
      @workers << Thread.new do
        worker_name = "index_worker_#{i}"
        DataCollector::Core.log("Starting index worker #{worker_name}")

        data = []
        last_log_time = 0
        last_force_save_time = 0

        while @running
          begin
            current_time = Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i

            # Batch queue items
            batch_data = get_queue_batch(data)

            # Log queue size every 30 seconds
            if should_log_queue_size?(current_time, last_log_time)
              # DataCollector::Core.log("Queued for indexing: #{@queue.size}")
              last_log_time = current_time
            end

            # Save data if batch is full or it's time for periodic save
            if should_save_data?(data, current_time, last_force_save_time)
              save_batch(data, worker_name)
              data.clear
              last_force_save_time = current_time if current_time % 30 == 0
            end

            # Only sleep if no data was processed
            sleep(batch_data > 0 ? 0.01 : 0.2)

          rescue StandardError => e
            DataCollector::Core.log("Error in worker #{worker_name}: #{e.message}")
            data.clear
          end
        end

        # Final save on shutdown
        save_batch(data, worker_name) unless data.empty?
        DataCollector::Core.log("Stopping index worker #{worker_name}")
      end
    end
  rescue StandardError => e
    DataCollector::Core.log("Error setting up workers: #{e.message}")
  end

  def get_queue_batch(data, max_items = 50)
    items_processed = 0

    max_items.times do
      begin
        raw_data = @queue.pop(true)
        if raw_data.is_a?(Array)
          data.concat(raw_data)
          items_processed += 1
        else
          DataCollector::Core.log("Invalid data format: #{raw_data.class}")
        end
      rescue ThreadError
        break # Queue empty
      end
    end

    items_processed
  end

  def should_log_queue_size?(current_time, last_log_time)
    current_time % 30 == 0 && last_log_time != current_time
  end

  def should_save_data?(data, current_time, last_force_save_time)
    data.size >= 100 ||
      (current_time % 30 == 0 && last_force_save_time != current_time)
  end

  def save_batch(data, worker_name)
    return if data.empty?

    DataCollector::Core.log("#{worker_name} saving #{data.size} items")
    self.index(data)
    DataCollector::Core.log("#{worker_name} save complete")
  end
end
