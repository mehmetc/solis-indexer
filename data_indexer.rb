$LOAD_PATH << '.'
require 'thread'
require 'active_support/all'
require 'lib/indexer'

STDOUT.sync = true

indexable_entities = DataCollector::ConfigFile[:services][:data_indexer][:entities]
listener = DataCollector::Pipeline.new(name: DataCollector::ConfigFile[:services][:data_indexer][:name] || 'indexer', uri: DataCollector::ConfigFile[:services][:data_indexer][:indexer][:in])
indexer = Indexer.new
service = Indexer::Metadata::Solis.new(indexer)
$running = true

begin
  listener.on_message do |_input, _output, id|

    DataCollector::Core.log("#{listener.name}: #{id}")
    File.readlines(id, chomp: true).each do |id|
      id = id.gsub('"','') # sanitize
      if id.eql?('*')
        prev_clock = 0
        indexer.recreate do
          begin
            entity = id.gsub(::Solis::Options.instance.get[:graph_name] ,'').split('/').first.classify
            DataCollector::Core.log("#{listener.name}: running #{entity}")
            service.for(id) do |data|
              next unless data
              break unless $running
              begin
                indexer.queue << data
                indexer.workers.each(&:wakeup) if indexer.queue.size > 1000 #force indexers
              rescue StandardError => e
                DataCollector::Core.error(e.message)
              end
              current_clock = Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i
              if current_clock.modulo(30) == 0 && prev_clock != current_clock
                DataCollector::Core.log("#{listener.name}: #{entity} ids in queue:#{indexer.queue.size}, load workers:#{service.workers.map(&:alive?).select{|s| s}.size}/#{service.workers.size}, index workers:#{indexer.workers.map(&:alive?).select{|s| s}.size}/#{indexer.workers.size}")
                prev_clock = current_clock
              end
            end

            while indexer.queue.size > 0
              begin
                sleep 5
                DataCollector::Core.log("#{listener.name}: resuming #{entity} ids in queue size:#{indexer.queue.size}, load workers:#{service.workers.map(&:alive?).select{|s| s}.size}/#{service.workers.size}, index workers:#{indexer.workers.map(&:alive?).select{|s| s}}/#{indexer.workers.size}")
              rescue StandardError => e
                retry if indexer.queue.size > 0
              end
            end
          rescue StandardError => e
            DataCollector::Core.error(e.message)
          end
        end

        puts JSON.pretty_generate(indexer.stats)
      else
        entity = id.split('/')[-2]&.singularize&.to_sym
        unless indexable_entities.include?(entity.to_s.classify)
          DataCollector::Core.log("Not indexing entity of type '#{entity.to_s.classify}' for #{id} ")
          next
        end
        DataCollector::Core.log("#{listener.name}: running #{entity}")
        if service
          service.for(id) do |data|
            raise StandardError, "\tNo data for #{id}" unless data
            begin
              indexer.index(data)
            rescue StandardError => e
              DataCollector::Core.error(e.message)
            end
          end
        else
          DataCollector::Core.error("\tDo not know how to process '#{id}'")
        end
      end
    end

    File.delete(id)
    DataCollector::Core.log("#{indexer.index_name} contains #{indexer.count} records")
  end

  listener.run
rescue StandardError => e
  DataCollector::Core.error("#{listener.name}: #{e.message}")
ensure
  $running = false
  DataCollector::Core.log("#{listener.name}: stopping")
  listener.stop if listener.running?
  indexer.stop
  service.stop
end


