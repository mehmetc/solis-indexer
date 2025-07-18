require 'data_collector'
require 'solis'
require 'thread'

class Indexer
  class Metadata
    class Generic
      def initialize(indexer)
        @indexer = indexer
        @loader_threads = []
        @loader_queue = Queue.new
        @running = true
        @config = ::Solis::ConfigFile[:services][:data_indexer]

        solis_conf = ::Solis::ConfigFile[:services][:data][:solis]
        @solis = ::Solis::Graph.new(::Solis::Shape::Reader::File.read(solis_conf[:shape]), solis_conf)
        setup_loader_workers(@config[:indexer][:workers])

        @entities = @config[:entities]
      end

      def for(key)
        @entity = entity_for(key)
        case key
        when '*'
          total = load_all
          prev_clock = 0
          while @loader_queue.size > 0
            begin
              current_clock = Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i
              if current_clock.modulo(30) == 0 && prev_clock != current_clock
                DataCollector::Core.log("Queued to be loaded: #{@loader_queue.size}/#{total}")
                prev_clock = current_clock
              end
              key = @loader_queue.pop
              begin
                data = load_by_id(key)
                yield data # apply_data_to_query_list(data)
              rescue StandardError => e
                puts e.backtrace.join("\n")
                raise Error::IndexError, "Failed to load(#{__LINE__}) #{key}: #{e.message}"
              end
            rescue StandardError => e
              DataCollector::Core.log("#{e.message}")
              retry if @loader_queue.size > 0
            end

          end
          DataCollector::Core.log("Done loading all records")
        else
          begin
            data = load_by_id(key)
            yield data
          rescue StandardError => e
            raise Error::IndexError, "Failed to load(#{__LINE__}) #{key}: #{e.message}"
          end
        end
      rescue StandardError => e
        puts e.backtrace.join("\n")
        raise Error::IndexError, "Failed to load(#{__LINE__}) #{key}: #{e.message}"
      end

      def stop
        @running = false
        @loader_threads.each do |worker|
          DataCollector::Core.log("Draining load worker #{worker[:name]}")
          worker.join(5) unless worker.stop?
        end
      end

      private

      def entity_for(key)
        key.gsub(::Solis::Options.instance.get[:graph_name], '').split('/').first.classify
      end

      def count()
        ::Solis::Query.run('', "SELECT (COUNT(distinct ?s) as ?count) FROM <#{::Solis::Options.instance.get[:graph_name]}> WHERE {?s ?p ?o ; a <#{::Solis::Options.instance.get[:graph_name]}#{@entity}>.}").first[:count].to_i
      end

      def load_by_id(id)
        # DataCollector::Core.log("Loading #{id}")
        entity = entity_for(id)

        base_url = "#{::Solis::ConfigFile[:services][:data_logic][:host]}#{::Solis::ConfigFile[:services][:data_logic][:base_path]}"
        url = "#{base_url}/graph?entity=#{entity}&id=#{id.split('/').last}&from_cache=0&depth=5"

        data = JSON.parse(HTTP.timeout(5).get(url).body)
        # File.open("#{@config[:indexer][:raw]}/#{id.split('/').last}.json", "w") do |f|
        #   f.write(data.to_json)
        # end

        data
      rescue StandardError => e
        raise Error::IndexError, "Failed to load #{id}: #{e}"
      end

      def load_all
        total_count = 0
        limit = 1000

        @entities.each do |entity|
          offset = 0
          @entity = entity
          DataCollector::Core.log("Loading all #{entity} entities")
          total = count()
          total_count += total
          DataCollector::Core.log("Found #{total} #{entity} entities")

          while offset < total
            run_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            DataCollector::Core.log("Reading #{offset} - #{offset + limit} of #{total_count}")
            q = "SELECT DISTINCT ?s FROM <#{::Solis::Options.instance.get[:graph_name]}> WHERE {?s ?p ?o ; a <#{::Solis::Options.instance.get[:graph_name]}#{@entity}>.} limit #{limit} offset #{offset}"
            ids = ::Solis::Query.run('', q).map { |m| m[:s] }
            # filename = "#{Time.new.to_i}-#{rand(100000)}"
            # File.open("#{@config[:indexer][:ids]}/#{filename}.txt", 'w') do |f|
            ids.each do |id|
              @loader_queue << id
              #    f.puts id
              offset += 1
              if offset.modulo(100) == 0
                DataCollector::Core.log("#{@entity} - #{offset}/#{total} in #{Process.clock_gettime(Process::CLOCK_MONOTONIC) - run_time} seconds")
                run_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              end
            end
            # end
          end
        end
        total_count
      end

      def apply_data_to_query_list(data)
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
                # sleep 5 if @loader_queue.size > 20
                begin
                  retries ||= 1
                  id = @loader_queue.pop
                  raw_data = load_by_id(id)
                  data = raw_data # apply_data_to_query_list(raw_data)
                  @indexer.queue << data
                  ##@indexer.index(data)
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
