require_relative 'generic'

Dir.glob("#{DataCollector::ConfigFile[:services][:data_indexer][:metadata]}/**/*.rb").each do |metadata|
  next if metadata =~ /generic/
  require "#{metadata}"
  puts "Loading metadata loader #{metadata}"
end

$LOADERS = {}