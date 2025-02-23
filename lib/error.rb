module Error
  class General < StandardError
  end

  class IndexError < General
  end

  class MetadataError < General
  end
end
