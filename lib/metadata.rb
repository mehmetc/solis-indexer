Dir.glob("#{File.split(__FILE__)[0]}/metadata/**/*.rb").each do |metadata|
  next if metadata =~ /generic/
  require "#{metadata}"
  puts "Loading metadata loader #{metadata}"
  #self.class.const_get('OdisData::Indexer::Metadata::Organisatie')
end