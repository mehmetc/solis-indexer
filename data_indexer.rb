$LOAD_PATH << '.'
require 'thread'
require 'active_support/all'
require 'lib/indexer'
require 'lib/metadata'

STDOUT.sync = true

listener = DataCollector::Pipeline.new(name: DataCollector::ConfigFile[:services][:data_indexer][:name] || 'indexer', uri: DataCollector::ConfigFile[:services][:data_indexer][:indexer][:in])
indexer = Indexer.new
$running = true
$LOADERS = {
  organisaties: Metadata::Organisatie.new(indexer),
  personen: Metadata::Persoon.new(indexer)
}

begin
  listener.on_message do |_input, _output, id|
    DataCollector::Core.log("#{listener.name}: #{id}")
    File.readlines(id, chomp: true).each do |id|
      if id.eql?('*')
        indexer.recreate do
          $LOADERS.each do |entity, service|
            DataCollector::Core.log("#{listener.name}: running #{entity}")
            service.for(id) do |data|
              next unless data
              break unless $running
              begin
                indexer.queue << data
              rescue StandardError => e
                DataCollector::Core.error(e.message)
              end
            end

            while indexer.queue.size > 0
              sleep 5
              DataCollector::Core.log("#{listener.name}: resuming #{entity} ids in queue #{indexer.queue.size}")
            end
          end
        end
      else
        entity = id.split('/')[-2]&.to_sym
        DataCollector::Core.log("#{listener.name}: running #{entity}")
        service = $LOADERS[entity]
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
  $LOADERS.each do |_entity, service|
    service.stop
  end
end


