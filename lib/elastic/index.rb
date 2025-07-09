require 'http'
require 'json'
require 'logger'
require_relative '../ext/array'

class Index
  attr_reader :name, :elastic, :type, :configuration, :logger

  def initialize(name, configuration_file_name, elastic = 'http://127.0.0.1:9200', logger = Logger.new(STDOUT))
    @logger = logger
    @name = name
    @elastic = elastic
    @configuration = load_configuration_file(configuration_file_name)
    @type = '_doc' #@configuration['mappings'].keys.first if @configuration
  end

  def exist?
    response = HTTP.head("#{@elastic}/#{@name}", headers: { 'Accept' => 'application/json' })
    response.code == 200
  end

  def create
    raise "Index #{@name} already exists" if exist?
    raise 'Configuration not loaded correctly' unless @configuration

    @logger.info "Creating index #{@name}"
    response = HTTP.put("#{@elastic}/#{@name}", body: @configuration.to_json,
                        headers: { 'Content-Type' => 'application/json' })
    @logger.info "status #{response.code}"
    return true if response.code == 200

    raise "Failed to create index #{@name} \n#{response.body}"
  end

  def delete(name = @name)
    raise "Index #{name} does not exist" unless exist?

    @logger.info "Deleting index #{name}"
    response = HTTP.delete("#{@elastic}/#{name}")

    return true if response.code == 200

    raise "Failed to delete index #{name} \n#{response.body}"
  end

  def delete_data(data, id = 'id', save_to_disk = false, id_prefix = '')
    raise "Index #{@name} does not exist" unless exist?
    raise "Index type is not set. Configuration not loaded" unless @type

    data = [data] unless data.is_a?(Array)
    if save_to_disk
      filename = "#{Time.new.to_i}-#{rand(100000)}"
      File.open("./ndjson/#{filename}.ndjson", "wb") do |f|
        f.puts data.to_ndjson(@name, id, "delete", id_prefix)
      end
    end

    @logger.info "Deleting #{data.size} from #{@name}/#{@type}"
    response = HTTP.post("#{@elastic}/_bulk",
                         headers: { 'Content-Type' => 'application/x-ndjson' },
                         body: data.to_ndjson(@name, id, "delete", id_prefix))

    body = JSON.parse(response.body.to_s)
    return body if response.code == 200
    return body unless body["errors"]

    raise "Failed to delete record from #{@name}, #{response.code}, #{response.body.to_s}"
  end

  def insert(data, id = 'id', save_to_disk = false)
    begin
      retries ||= 0

      raise "Index #{@name} does not exist" unless exist?
      raise "Index type is not set. Configuration not loaded" unless @type
      raise "Data can not be nil" if data.nil?

      data = [data] unless data.is_a?(Array)

      return '' if data.nil? || data.empty?

      filename = "#{Time.new.to_i}-#{rand(100000)}"
      if save_to_disk
        File.open("./ndjson/#{filename}.ndjson", "wb") do |f|
          f.puts data.to_ndjson(@name, id, "index")
        end
      end

      @logger.info "Inserting #{data.size} into #{@name}/#{@type}"
      response = HTTP.post("#{@elastic}/_bulk",
                           headers: { 'Content-Type' => 'application/x-ndjson' },
                           body: data.to_ndjson(@name, id, "index"))

      response_body = JSON.parse(response.body.to_s)
      raise "Failed to insert record into #{@name}, #{"./ndjson/#{filename}.ndjson"} - #{response.code}" if response.code != 200
      raise "Failed to insert record into #{@name}" if response_body["errors"]

      response_body
    rescue StandardError => e
      if response_body["errors"]
        @logger.info "Removing bad records and trying again"
        response_body['items'].select { |m| m['index']['status'] != 201 }.each do |error|
          id =  error["index"]["_id"]
          error_type = error["index"]["error"]["type"]
          error_reason = error["index"]["error"]["reason"]
          @logger.error "#{id} - #{error_type}: #{error_reason}"
          data.delete_if{|d| d.dig('fiche','_id').eql?(id)}
        end

        retry if (retries += 1) < 2
      end
      raise e
    end
  rescue StandardError => e
    retry if errors == {}

    response_body['items'].select { |m| m['index']['status'] != 201 }.each { |m| d = m['index']; errors[d['_id']] = d['error'] }
    raise e, errors.to_json
  end

  def get_by_id(id)
    response = HTTP.get("#{@elastic}/#{@name}/#{@type}/#{id}")
    return response.body if response.code == 200

    raise "Failed to load record #{id} form #{@name} index"
  end

  def count
    response = HTTP.get("#{@elastic}/#{@name}/_count")
    if response.code == 200
      data = JSON.parse(response.body)
      return data['count']
    end

    raise "Failed to count records in #{@name} index"
  end

  private

  def load_configuration_file(configuration_file_name)
    raise 'Configuration file not found' unless File.exist?(configuration_file_name)
    JSON.parse(File.read(configuration_file_name))
  end
end

