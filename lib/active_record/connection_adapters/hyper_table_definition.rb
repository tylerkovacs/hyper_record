module ActiveRecord
  module ConnectionAdapters #:nodoc:
    class HyperColumnDefinition < Struct.new(:base, :name, :type, :limit, :max_versions, :options) #:nodoc:
    end

    class HyperTableDefinition < TableDefinition
      def column(name, type, options = {})
        column = self[name] || HyperColumnDefinition.new(@base, name, type)
        if options[:limit]
          column.limit = options[:limit]
        elsif native[type.to_sym].is_a?(Hash)
          column.limit = native[type.to_sym][:limit]
        end
        column.max_versions = options[:max_versions]
        column.options = options
        @columns << column unless @columns.include? column
        self
      end
    end
  end
end

