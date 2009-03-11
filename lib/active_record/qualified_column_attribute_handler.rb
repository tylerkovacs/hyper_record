module ActiveRecord
  # QualifiedColumnAttributeHandler acts as a proxy for qualified columns
  # within the @attributes instance variable.  If a model has a qualified
  # column called 'misc':
  #
  # o = Model.new
  # o.misc['new_value'] = 1
  #
  # Then @attributes['misc'] is a QualifiedColumnAttributeHandler that
  # proxies the request for o.misc['new_value'] to 
  # @attributes['misc:url']
  #
  # Similar logic exists for setting values.
  class QualifiedColumnAttributeHandler < Hash
    attr_accessor :column_family, :model

    def initialize(model, column_family, default)
      super(nil)
      self.default = default
      @model = model
      @column_family = column_family
      @default = default
      self
    end

    def [](attr_name)
      super(attr_name)
      col = @model.connection.qualified_column_name(@column_family, attr_name)
      @model.read_attribute(col)
    end

    def []=(attr_name, value)
      super(attr_name, value)
      col = @model.connection.qualified_column_name(@column_family, attr_name)
      @model.write_attribute(col, value)
      value
    end

    def delete(attr_name)
      super(attr_name)
      col = @model.connection.qualified_column_name(@column_family, attr_name)
      @model.remove_attribute(col)
    end

    def set_hash(attrs)
      self.clear
      self.merge!(attrs)
      self
    end

    def get_hash
      self
    end

    def reset_from_attribute_values
      new_hash = {}

      attributes = @model.instance_eval("@attributes")
      attributes.keys.select{|k| k =~ /^#{@column_family}:/ }.each{|key|
        column_family, column_qualifier = key.split(':')
        new_hash[column_qualifier] = attributes[key].first['value']
      }

      set_hash(new_hash)
    end
  end
end
