require_relative 'generic'

class Indexer
  class Metadata
    class Solis < Indexer::Metadata::Generic
      def initialize(indexer)
        super(indexer)
      end
    end
  end
end