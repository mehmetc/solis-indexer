require 'data_collector'
require 'solis'
require 'thread'

class Indexer
  class Metadata
    class Generic
      def initialize(indexer, entity, construct)
        #@config = DataCollector::ConfigFile
        @indexer = indexer
        @entity = entity
        @entity_id = "#{entity.underscore}_id"
        @filename = construct
        @loader_threads = []
        @loader_queue = Queue.new
        @running = true

        @config = DataCollector::ConfigFile[:services][:data_indexer]
        solis_conf = Solis::ConfigFile[:services][:data][:solis]
        @solis = Solis::Graph.new(Solis::Shape::Reader::File.read(solis_conf[:shape]), solis_conf)
        setup_loader_workers(@config[:indexer][:workers])
      end

      def for(key)
        case key
        when '*'
          load_all
          total = count()
          run_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          while @loader_queue.size != 0
            if (Process.clock_gettime(Process::CLOCK_MONOTONIC) - run_time).to_i.modulo(30) == 0
              DataCollector::Core.log("To be loaded: #{@loader_queue.size}/#{total}")
            end
            key = @loader_queue.pop
            data = load_by_id(key)
            yield apply_data_to_query_list(data, @entity)
          end
          DataCollector::Core.log("Done loading all records")
        else
          data = load_by_id(key)
          yield apply_data_to_query_list(data, @entity)
        end
      end

      def stop
        @running = false
        @loader_threads.each do |worker|
          DataCollector::Core.log("Draining load worker #{worker[:name]}")
          worker.join(5) unless worker.stop?
        end
      end

      private

      def count()
        Solis::Query.run('', "SELECT (COUNT(distinct ?s) as ?count) FROM <#{Solis::Options.instance.get[:graph_name]}> WHERE {?s ?p ?o ; a <#{Solis::Options.instance.get[:graph_name]}#{@entity}>.}").first[:count].to_i
      end

      def load_by_id(id)
        DataCollector::Core.log("Loading #{id}")
        # data = Solis::Query.run(@entity, "SELECT * FROM <#{Solis::Options.instance.get[:graph_name]}> WHERE {<#{id}> ?p ?o ; a <#{Solis::Options.instance.get[:graph_name]}#{@entity}>.}")
        data = Solis::Query.run_construct_with_file(@filename, @entity_id, @entity, id, 0)
        File.open("#{@config[:indexer][:raw]}/#{id.split('/').last}.json", "w") do |f|
          f.write(data.to_json)
        end

        data
      rescue StandardError => e
        raise Error::IndexError, "Failed to load #{id}: #{e}"
      end

      def load_all
        limit = 1000
        offset = 0
        total = count()

        while offset < total
          run_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          DataCollector::Core.log("Reading #{offset} - #{offset + limit} of #{total}")
          q = "SELECT DISTINCT ?s FROM <#{Solis::Options.instance.get[:graph_name]}> WHERE {?s ?p ?o ; a <#{Solis::Options.instance.get[:graph_name]}#{@entity}>.} limit #{limit} offset #{offset}"
          ids = Solis::Query.run('', q).map { |m| m[:s] }
          File.open("#{@config[:indexer][:ids]}/#{offset}.txt", 'w') do |f|
            ids.each do |id|
              @loader_queue << id
              f.puts id
              offset += 1
              if offset.modulo(100) == 0
                DataCollector::Core.log("#{offset}/#{total} in #{Process.clock_gettime(Process::CLOCK_MONOTONIC) - run_time} seconds")
                run_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              end
            end
          end
        end
      end

      def apply_data_to_query_list(data, entity)
        new_data = []
        return new_data if data.nil?
        data.each do |d|
          id = DataCollector::Core.filter(d, '$.id').first

          query_list.each do |index_key, v|
            v = [v] unless v.is_a?(Array)
            nd = []
            v.each do |w|
              w.each do |i, j|
                j = [j] unless j.is_a?(Array)
                j.each do |k|
                  if k.is_a?(Hash)
                    sout = {}
                    k.each { |a, b|
                      t = DataCollector::Core.filter(d, b)
                      t.each do |s|
                        if sout.key?(a)
                          sd = sout[a].is_a?(Array) ? sout[a] : [sout[a]]
                          sd << s

                          sout[a] = sd
                        else
                          sout[a] = s
                        end
                      end
                    }
                    #.hash.abs.to_s(36)

                    nd << { index_key => { 'id' => id, i => sout } }
                  else
                    t = DataCollector::Core.filter(d, k)
                    t.each do |s|
                      if i.eql?('datering') || i.eql?('datering_systematisch')
                        gte, lte = s.to_s.split('/')
                        if s.is_a?(ISO8601::TimeInterval)
                          if s.size < 0
                            lte = s.start_time.strftime('%Y-%m-%dT%H:%M:%S.%L%z').to_datetime.utc.iso8601
                            gte = s.end_time.strftime('%Y-%m-%dT%H:%M:%S.%L%z').to_datetime.utc.iso8601
                          else
                            gte = s.start_time.strftime('%Y-%m-%dT%H:%M:%S.%L%z').to_datetime.utc.iso8601
                            lte = s.end_time.strftime('%Y-%m-%dT%H:%M:%S.%L%z').to_datetime.utc.iso8601
                          end

                          nd << { index_key => { 'id' => id, i => { 'gte' => gte, 'lte' => lte } } }
                        elsif s.is_a?(Array)
                          s.uniq!
                          s.each do |e|
                            gte, lte = e.to_s.split('/')
                            if e.is_a?(ISO8601::TimeInterval)
                              if e.size < 0
                                lte = e.start_time.strftime('%Y-%m-%dT%H:%M:%S.%L%z').to_datetime.utc.iso8601
                                gte = e.end_time.strftime('%Y-%m-%dT%H:%M:%S.%L%z').to_datetime.utc.iso8601
                              else
                                gte = e.start_time.strftime('%Y-%m-%dT%H:%M:%S.%L%z').to_datetime.utc.iso8601
                                lte = e.end_time.strftime('%Y-%m-%dT%H:%M:%S.%L%z').to_datetime.utc.iso8601
                              end
                            end
                            nd << { index_key => { 'id' => id, i => { 'gte' => gte, 'lte' => lte } } }
                          end
                        else
                          begin
                            intervals = ISO8601::TimeInterval.from_datetimes(ISO8601::DateTime.new(gte), ISO8601::DateTime.new(lte))
                            if intervals.size < 0
                              nd << { index_key => { 'id' => id, i => { 'gte' => intervals.end_time.to_datetime.utc.iso8601, 'lte' => intervals.start_time.to_datetime.utc.iso8601 } } }
                            else
                              nd << { index_key => { 'id' => id, i => { 'gte' => intervals.start_time.to_datetime.utc.iso8601, 'lte' => intervals.end_time.to_datetime.utc.iso8601 } } }
                            end
                          rescue StandardError => e
                            nd << { index_key => { 'id' => id, i => { 'gte' => gte, 'lte' => lte } } }
                          end
                        end
                      else
                        nd << { index_key => { 'id' => id, i => s } }
                      end
                    end
                  end
                end
              end
            end

            if v.length == 1
              new_data.concat(nd)
            else
              tnd = {}
              nd.map { |m| m[index_key] }.compact.map { |m| m.delete_if { |k, v| k.eql?('id') } }.each { |e|
                if tnd.key?(e.keys.first)
                  a = tnd[e.keys.first]
                  # if a.is_a?(Hash)
                  #   a = a.merge(e.values.first)
                  # else
                  a = [a] unless a.is_a?(Array)
                  a << e.values.first
                  # end

                  tnd[e.keys.first] = a
                else
                  tnd[e.keys.first] = e.values.first
                end
              }

              new_data << { index_key => { 'id' => id }.merge(tnd) }
            end
          end

          # new_data << { "#{entity.underscore}" => d }
        end
        new_data.each do |fiche|
          if fiche['fiche']['data']['datering_systematisch'].is_a?(Array)
            fiche['fiche']['data']['datering_systematisch'].each do |fiche_item|
              fiche_item = fiche_item.to_s unless fiche_item.is_a?(String)
            end
          elsif fiche['fiche']['data']['datering_systematisch']
            fiche['fiche']['data']['datering_systematisch'] = fiche['fiche']['data']['datering_systematisch'].to_s unless fiche['fiche']['data']['datering_systematisch'].is_a?(String)
          end

          if fiche['fiche']['data'].key?('agent') && !fiche['fiche']['data']['agent'].empty?
            fiche['fiche']['data']['agent'].each do |agent|
              agent['datering_systematisch'] = agent['datering_systematisch'].to_s unless agent['datering_systematisch'].is_a?(String)
            end
          end

        end

        new_data
      rescue StandardError => e
        puts e.message
      end

      def query_list
        raise Error::Error, 'To be implemented'
        {}
      end

      def setup_loader_workers(number_of_workers)
        number_of_workers.times do |i|
          @loader_threads << Thread.new do
            Thread.current[:name] = i
            DataCollector::Core.log("Starting load worker #{Thread.current[:name]}")
            while @running
              if @loader_queue.size > 0
                sleep 5 if @loader_queue.size > 20
                begin
                  retries ||= 1
                  id = @loader_queue.pop
                  raw_data = load_by_id(id)
                  data = apply_data_to_query_list(raw_data, @entity)
                  @indexer.index(data)
                rescue StandardError => e
                  if retries < 3
                    DataCollector::Core.log("Retrying #{retries}/3 in 10 sec.: #{e.message}")
                    sleep 10
                    retries += 1
                    retry
                  end
                end
              end
            end
            DataCollector::Core.log("Stopping load worker #{Thread.current[:name]}")
          end
        end
      end
    end
  end
end
