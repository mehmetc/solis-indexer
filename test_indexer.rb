$LOAD_PATH << '.' << '/Users/mehmetc/Dropbox/AllSources/ego/solis-project/stack/indexer/'
require 'solis'
require 'indexer/lib/indexer'
require 'config/elastic/search/search_rules'

solis_conf = ::Solis::ConfigFile[:services][:data][:solis]
solis = ::Solis::Graph.new(::Solis::Shape::Reader::File.read(solis_conf[:shape]), solis_conf)

indexer = Indexer.new
#input = DataCollector::Input.new.from_uri("https://data.q.archiefpunt.be/_logic/graph?entity=Beheerder&id=BH01-B901-A195-9960-BABD3217BW9A&from_cache=0&depth=5", headers: {"Authorization" => "Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1c2VyIjoiU1lTVEVNIiwicm9sZSI6WyJmdWxsIl19.jjX8nSg8OwAlbXHXkF5ASJMxwHDAlu0SaeHszLTSxIWvyhXzm5tYQMqHlzFP9DOrpfeN2VLF9Xw25NcoERS4GA"})
#input = DataCollector::Input.new.from_uri("https://data.q.archiefpunt.be/_logic/graph?entity=Archief&id=1007-A85C-0B7F-8196-B87211113AE9&from_cache=0&depth=5", headers: {"Authorization" => "Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1c2VyIjoiU1lTVEVNIiwicm9sZSI6WyJmdWxsIl19.jjX8nSg8OwAlbXHXkF5ASJMxwHDAlu0SaeHszLTSxIWvyhXzm5tYQMqHlzFP9DOrpfeN2VLF9Xw25NcoERS4GA"})
input = DataCollector::Input.new.from_uri("https://data.q.archiefpunt.be/_logic/graph?entity=Samensteller&id=SS02-A606-9840-9D96-B7AF88500AR9&from_cache=0&depth=5", headers: {"Authorization" => "Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1c2VyIjoiU1lTVEVNIiwicm9sZSI6WyJmdWxsIl19.jjX8nSg8OwAlbXHXkF5ASJMxwHDAlu0SaeHszLTSxIWvyhXzm5tYQMqHlzFP9DOrpfeN2VLF9Xw25NcoERS4GA"})

processed_data = indexer.process(input)

out  = DataCollector::Output.new
#processed_data.each do |data|
data = { "hits" => {"total" => {"value" => 1},"hits" => {"_source" => processed_data.first}}}
DataCollector::Core.rules.run(RULES["solis"], data, out, { "_no_array_with_one_element" => true, query: {from: 0, size: 10} })
#end

puts JSON.pretty_generate(out.raw)
#puts JSON.pretty_generate(processed_data)