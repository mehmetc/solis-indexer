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

  def initialize
    @running = true
    @queue = Queue.new
    @config = DataCollector::ConfigFile[:services][:data_indexer]
    @elastic = setup_elastic
    @workers = []
    @stats = {'load' => {}, 'error' => {}}
    @stats_mutex = Mutex.new
    @shutdown_timeout = 30 # seconds

    # Dynamic batching configuration
    @min_batch_size = @config.dig(:indexer, :min_batch_size) || 10
    @max_batch_size = @config.dig(:indexer, :max_batch_size) || 500
    @target_processing_time = @config.dig(:indexer, :target_processing_time) || 5.0 # seconds
    @batch_adjustment_factor = 0.2 # How aggressively to adjust batch size

    setup_index_workers(@config[:elastic][:workers])
  end

  def index(data)
    return unless data && !data.empty?

    index_data = process(data)
    return if index_data.empty?

    update_load_stats(index_data)

    result = @elastic.index.insert(index_data, 'fiche.data._id', false)
    validate_index_result(result, index_data)

    data.clear
  rescue StandardError => e
    data.clear
    handle_index_error(e)
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

    swap_indices
  end

  def running?
    @running
  end

  def stop
    DataCollector::Core.log("Stopping indexer...")
    @running = false

    # Signal all workers to stop and wait for them
    @workers.each do |worker|
      worker_name = worker[:name] || "unnamed_worker"
      DataCollector::Core.log("Stopping index worker #{worker_name}")

      begin
        if worker.join(@shutdown_timeout)
          DataCollector::Core.log("Worker #{worker_name} stopped gracefully")
        else
          DataCollector::Core.log("Worker #{worker_name} didn't stop within timeout, terminating")
          worker.terminate
        end
      rescue => e
        DataCollector::Core.log("Error stopping worker #{worker_name}: #{e.message}")
      end
    end

    DataCollector::Core.log("All workers stopped")
  end

  def process(metadata)
    return [] if metadata.nil? || metadata.empty? || !running?

    rule_name = determine_rule_name(metadata)
    rules = get_rules(rule_name)

    result = []
    output = DataCollector::Output.new

    metadata.each do |data|
      output.clear
      DataCollector::Core.rules.run(rules, data, output, {
        "_no_array_with_one_element" => true,
        solis: Solis::Options.instance
      })
      result << output.raw
    end

   result.each(&:deep_stringify_keys!)
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

  def setup_index_workers(number_of_workers)
    number_of_workers.times do |i|
      worker = Thread.new do
        worker_name = "index_worker_#{i}"
        Thread.current[:name] = worker_name
        DataCollector::Core.log("Starting index worker #{worker_name}")

        run_worker(worker_name)

        DataCollector::Core.log("Index worker #{worker_name} stopped")
      end

      @workers << worker
    end
  rescue StandardError => e
    DataCollector::Core.log("Error setting up workers: #{e.message}")
  end

  def run_worker(worker_name)
    batch = []
    last_save_time = Time.now
    last_log_time = Time.now
    save_interval = 10 # seconds - reduced for faster processing
    log_interval = 30 # seconds
    current_batch_size = @min_batch_size
    processing_times = [] # Track recent processing times for dynamic adjustment

    while @running
      begin
        queue_size = @queue.size

        # Determine optimal batch size based on queue pressure and processing time
        optimal_batch_size = calculate_optimal_batch_size(
          queue_size,
          current_batch_size,
          processing_times
        )

        # Collect items up to optimal batch size
        items_added = collect_batch_items(batch, optimal_batch_size)

        current_time = Time.now

        # Log status periodically
        if current_time - last_log_time >= log_interval
          avg_processing_time = processing_times.empty? ? 0 : processing_times.sum / processing_times.size
          log_worker_status(worker_name, batch.size, queue_size, optimal_batch_size, avg_processing_time)
          last_log_time = current_time
        end

        # Decide when to save batch
        should_save = should_save_batch?(batch, current_time, last_save_time, save_interval, queue_size)

        if should_save
          processing_start = Time.now
          save_batch(batch, worker_name)
          processing_time = Time.now - processing_start

          # Track processing times (keep last 10 measurements)
          processing_times << processing_time
          processing_times.shift if processing_times.size > 10

          # Update current batch size based on performance
          current_batch_size = optimal_batch_size
          last_save_time = current_time
        end

        # Smart sleeping based on queue state and batch size
        sleep_time = calculate_sleep_time(items_added, batch.size, queue_size)
        sleep(sleep_time) if sleep_time > 0

      rescue StandardError => e
        DataCollector::Core.log("Error in worker #{worker_name}: #{e.message}")
        DataCollector::Core.log(e.backtrace.join("\n")) if e.backtrace
        sleep(1) # Brief pause on error
      end
    end

    # Final save on shutdown
    save_batch(batch, worker_name) unless batch.empty?
  end

  def collect_batch_items(batch, max_items)
    items_added = 0
    max_items_to_add = max_items - batch.size

    max_items_to_add.times do
      begin
        raw_data = @queue.pop(true) # non-blocking pop

        if raw_data.is_a?(Array)
          batch.concat(raw_data)
          items_added += 1
        else
          DataCollector::Core.log("Invalid data format in queue: #{raw_data.class}")
        end
      rescue ThreadError
        # Queue is empty
        break
      end
    end

    items_added
  end

  def save_batch(batch, worker_name)
    return if batch.empty?

    batch_size = batch.size
    DataCollector::Core.log("#{worker_name} saving #{batch_size} items")

    begin
      index(batch)
      DataCollector::Core.log("#{worker_name} successfully saved #{batch_size} items")
    rescue => e
      DataCollector::Core.log("#{worker_name} failed to save batch: #{e.message}")
      raise
    end
  end

  def log_worker_status(worker_name, batch_size, queue_size, optimal_batch_size, avg_processing_time)
    alive_workers = @workers.count(&:alive?)
    total_workers = @workers.size

    DataCollector::Core.log(
      "#{worker_name}: batch=#{batch_size}, queue=#{queue_size}, " \
        "optimal_batch=#{optimal_batch_size}, avg_time=#{avg_processing_time.round(2)}s, " \
        "workers=#{alive_workers}/#{total_workers}"
    )
  end

  def update_load_stats(index_data)
    @stats_mutex.synchronize do
      load_counts = DataCollector::Core.filter(index_data, '$[*].fiche.data._id')
                                       .map { |id| id.split('/')[-2] }
                                       .tally
      @stats['load'].merge!(load_counts) { |_key, old_val, new_val| old_val + new_val }
    end
  end

  def validate_index_result(result, index_data)
    if result.empty? || result['items'].size != index_data.size
      all_in_ids = DataCollector::Core.filter(index_data, '$[*].fiche.data._id').sort
      all_out_ids = (result['items']&.map { |m| m['index']['_id'] } || []).sort
      missing = all_in_ids - all_out_ids

      if missing.size > 0
        raise Error::IndexError, "Missing IDs after indexing: #{missing.join(', ')}"
      end
    end
  end

  def handle_index_error(error)
    @stats_mutex.synchronize do
      begin
        errors = JSON.parse(error.message)
        if errors.is_a?(Hash)
          error_counts = errors.keys.map { |id| id.split('/')[-2] }.tally
          @stats['error'].merge!(error_counts) { |_key, old_val, new_val| old_val + new_val }
        end
      rescue JSON::ParserError
        # Error message is not JSON, skip stats update
      end
    end
  end

  def determine_rule_name(metadata)
    return 'GENERIC_RULES' if metadata.empty?

    begin
      id_field = metadata.first['id'] || metadata.first['_id']
      return 'GENERIC_RULES' unless id_field

      "#{id_field.split('/')[-2].classify.upcase}_RULES"
    rescue StandardError
      'GENERIC_RULES'
    end
  end

  def get_rules(rule_name)
    unless Object.const_defined?(rule_name)
      DataCollector::Core.log("Rule #{rule_name} not found. Available rules: #{Object.constants.grep(/RULES/).join(', ')}")
      rule_name = 'GENERIC_RULES'
    end

    Object.const_get(rule_name)
  end

  def swap_indices
    DataCollector::Core.log("Attaching alias to new index: #{@new_index_name}:#{@index_alias}")
    @elastic.alias.replace(@index_alias, @current_index_name, @new_index_name)

    DataCollector::Core.log("Removing old index: #{@current_index_name}")
    @elastic.index.delete(@current_index_name)

    DataCollector::Core.log("#{@elastic.index.count} records indexed")
  end

  # Dynamic batch sizing methods
  def calculate_optimal_batch_size(queue_size, current_batch_size, processing_times)
    # Start with current batch size
    optimal_size = current_batch_size

    # Adjust based on queue pressure (if queue is growing, increase batch size)
    if queue_size > 1000
      # High pressure - increase batch size aggressively
      pressure_multiplier = [1.5, (queue_size / 1000.0)].min
      optimal_size = (optimal_size * pressure_multiplier).round
    elsif queue_size > 100
      # Medium pressure - increase batch size moderately
      optimal_size = (optimal_size * 1.2).round
    elsif queue_size < 10
      # Low pressure - decrease batch size to reduce latency
      optimal_size = (optimal_size * 0.9).round
    end

    # Adjust based on processing time if we have data
    unless processing_times.empty?
      avg_time = processing_times.sum / processing_times.size

      if avg_time > @target_processing_time
        # Processing too slow - reduce batch size
        time_factor = 1.0 - (@batch_adjustment_factor * (avg_time - @target_processing_time) / @target_processing_time)
        optimal_size = (optimal_size * [time_factor, 0.5].max).round
      elsif avg_time < @target_processing_time * 0.7
        # Processing fast - increase batch size
        time_factor = 1.0 + (@batch_adjustment_factor * (@target_processing_time - avg_time) / @target_processing_time)
        optimal_size = (optimal_size * [time_factor, 1.5].min).round
      end
    end

    # Ensure we stay within bounds
    [[optimal_size, @min_batch_size].max, @max_batch_size].min
  end

  def should_save_batch?(batch, current_time, last_save_time, save_interval, queue_size)
    return false if batch.empty?

    # Always save if batch is at max size
    return true if batch.size >= @max_batch_size

    # Save if minimum batch size reached and time interval passed
    time_elapsed = current_time - last_save_time
    return true if batch.size >= @min_batch_size && time_elapsed >= save_interval

    # Save immediately if queue is growing too fast (pressure relief)
    return true if queue_size > 2000 && batch.size >= @min_batch_size

    # Save if we have a reasonable batch and queue pressure is high
    return true if queue_size > 500 && batch.size >= (@min_batch_size / 2) && time_elapsed >= (save_interval / 2)

    false
  end

  def calculate_sleep_time(items_added, batch_size, queue_size)
    # No sleep if we added items or have work to do
    return 0 if items_added > 0 || batch_size > 0

    # Adaptive sleep based on queue state
    if queue_size > 100
      0.01 # Very short sleep when queue has items
    elsif queue_size > 10
      0.05 # Short sleep when some items in queue
    else
      0.1 # Normal sleep when queue is nearly empty
    end
  end
end