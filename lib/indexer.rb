require 'data_collector'
require 'lib/elastic'
require 'thread'
require_relative 'error'
require 'lib/indexer/metadata'

Dir.glob('./config/rules/*_rules.rb').each do |rule|
  puts "Loading rule #{rule}"
  require "#{rule}"
end

  class Indexer
    attr_reader :index_name, :queue
    def initialize()
      @running = true
      @queue = Queue.new
      @config = DataCollector::ConfigFile[:services][:data_indexer]
      @elastic = setup_elastic
      @index_workers = []

      setup_index_workers(@config[:elastic][:workers])
    end

    def index(data)
      if data
        @elastic.index.insert(data, 'fiche.id', false)
      end
    rescue => e
      puts data
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
      #TODO
      output = DataCollector::Output.new
      @rules.run(ORGANIZATION_RULES, metadata, output, { "_no_array_with_one_element" => true })

      output.raw
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
          Thread.current[:name]=i
          DataCollector::Core.log("Starting index worker #{Thread.current[:name]}")
          data = []
          run_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          while @running
            if @queue.size > 0
              data += @queue.pop
              if data.size > 10 || (Process.clock_gettime(Process::CLOCK_MONOTONIC) - run_time).to_i > 30
                self.index(data)
                data = []
                run_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              end
            else
              sleep 10
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
