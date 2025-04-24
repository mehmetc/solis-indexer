require 'data_collector'
require 'lib/elastic'
require 'thread'
require_relative 'error'
require 'lib/indexer/solis'

Dir.glob('./config/rules/*_rules.rb').each do |rule|
  puts "Loading rule #{rule}"
  require "#{rule}"
end

class Indexer
  attr_reader :index_name, :queue, :stats

  def initialize()
    @running = true
    @queue = Queue.new
    @config = DataCollector::ConfigFile[:services][:data_indexer]
    @elastic = setup_elastic
    @index_workers = []
    @stats = {'load' => {}, 'error' => {}}

    setup_index_workers(@config[:elastic][:workers])
  end

  def index(data)
    if data
      data = process(data)
      @stats['load'].merge!(DataCollector::Core.filter(data, '$[*].fiche.data._id').map{|m| m.split('/')[-2]}.tally){|k,o,n| o+n}
      result = @elastic.index.insert(data, 'fiche.data._id', false)

      if result['items'].size != data.size
        all_in_ids = DataCollector::Core.filter(data, '$[*].fiche.data._id').sort
        all_out_ids = result['items'].map{|m| m['index']['_id']}.sort
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
    @index_workers.each do |worker|
      DataCollector::Core.log("Draining index worker #{worker[:name]}")
      worker.join(5) unless worker.stop?
    end
  end

  private

  def process(metadata)
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
      rules = Object.const_get(rule_name)
    rescue StandardError => e
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
      @index_workers << Thread.new do
        Thread.current[:name] = i
        DataCollector::Core.log("Starting index worker #{Thread.current[:name]}")
        data = []
        run_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        while @running
          begin
            current_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            if @queue.size > 0
              data += @queue.pop
              if data.size > 100 || (current_time - run_time).to_i > 30
                self.index(data)
                data = []
                run_time = current_time
              end
            else
              sleep 10
            end
          rescue StandardError => e
            data = []
            DataCollector::Core.log("Error indexing: #{e.message}")
          end
        end
        self.index(data) if data.size > 0
        DataCollector::Core.log("Stopping index worker #{Thread.current[:name]}")
      end
    end
  rescue StandardError => e
    DataCollector::Core.log("Error indexing: #{e.message}")
  end
end
