module ActiveRecord
  module HyperAttributeMethods #:nodoc:
    def self.included(base)
      base.extend ClassMethods
    end

    # Returns the value of the attribute identified by <tt>attr_name</tt>
    # after it has been typecast (for example, "2004-12-12" in a data column
    # is cast to a date object, like Date.new(2004, 12, 12)).
    #
    # read_attributes gets all information from the @attributes instance
    # variable.  HyperRecord objects store cell metadata alongside the
    # value in @attributes, so additional logic is required to distinguise
    # between requests for the cell value and requests for cell metadata.
    # read_attribute returns just the cell value (see raw_attribute for
    # accessing metadata)
    def read_attribute(attr_name)
      attr_name = attr_name.to_s
      cell_values = @attributes[attr_name]
      column = column_for_attribute(attr_name)

      if cell_values
        if cell_values.is_a?(Array)
          value = cell_values.first && (value = cell_values.first['value'])
        else
          value = cell_values
        end
      else
        value = nil
      end

      if value
        if column
          if unserializable_attribute?(attr_name, column)
            unserialize_attribute(attr_name)
          elsif column.is_a?(ConnectionAdapters::QualifiedColumn)
            value.reset_from_attribute_values
            value
          else
            column.type_cast(value)
          end
        else
          value
        end
      elsif self.class.qualified?(attr_name) && column
        value = QualifiedColumnAttributeHandler.new(self, column.name, column.default)
        value.reset_from_attribute_values
        value
      else
        nil
      end
    end

    # Return the raw attribute complete with metadata.
    # => {"new"=>false, "timestamp"=>1236656081741323001, "column_family"=>"name", "row_key"=>"page_1", "revision"=>-9223372036854775806, "flag"=>255, "column_qualifier"=>nil, "value"=>"LOLcats and more"}
    def raw_attribute(attr_name)
      !@attributes[attr_name].blank? ? @attributes[attr_name].first : nil
    end

    # Override base read_attribute_before_type_cast to access attribute
    # value instead of complete cell metadata.
    def read_attribute_before_type_cast(attr_name)
      @attributes[attr_name]['value']
    end

    # Updates the attribute identified by <tt>attr_name</tt> with the
    # specified +value+. Empty strings for fixnum and float columns are
    # turned into +nil+.
    def write_attribute(attr_name, value)
      if self.class.qualified?(attr_name) and !value.is_a?(Hash)
        raise "Can't assign #{value.class} to a qualified_column.  Either make the column unqualified or assign to it as a Hash."
      else
        cell_values = @attributes[attr_name]
        if cell_values
          if cell_values.is_a?(QualifiedColumnAttributeHandler)
            value.keys.each{|key| write_attribute([cell_values.column_family, key].join(':'), value[key])}
          elsif cell_values.first && cell_values.first['new']
            cell_values.first['value'] = value
          else
            add_new_cell(attr_name, value)
          end
        else
          add_new_cell(attr_name, value)
        end
      end
    end

    # Remove an attribute from the @attributes instance variable.
    def remove_attribute(attr_name)
      @attributes.delete(attr_name)
    end

    def add_new_cell(attr_name, value)
      iqcn = self.connection.is_qualified_column_name?(attr_name)
      status, column_family, column_qualifier = iqcn
      column_family = attr_name if !status

      @attributes[attr_name] = [] if !@attributes[attr_name].is_a?(Array)
      @attributes[attr_name].unshift({
        'row_key' => self.ROW,
        'column_family' => column_family,
        'column_qualifier' => column_qualifier,
        'value' => value,
        'new' => true
      })
      value
    end

    module ClassMethods
      # Override base define_read_method to access attribute
      # value instead of complete cell metadata.
      def define_read_method(symbol, attr_name, column)
        cast_code = column.type_cast_code('v') if column
        access_code = cast_code ? "(v=@attributes['#{attr_name}']) && #{cast_code}" : "read_attribute('#{attr_name}')"

        unless attr_name.to_s == self.primary_key.to_s
          access_code = access_code.insert(0, "missing_attribute('#{attr_name}', caller) unless @attributes.has_key?('#{attr_name}'); ")
        end

        evaluate_attribute_method attr_name, "def #{symbol}; #{access_code}; end"
      end

      # Evaluate the definition for an attribute related method
      def evaluate_attribute_method(attr_name, method_definition, method_name=attr_name)
        unless method_name.to_s == primary_key.to_s
          generated_methods << method_name
        end

        begin
          class_eval(method_definition, __FILE__, __LINE__)
        rescue SyntaxError => err
          generated_methods.delete(attr_name)
          if logger
            logger.warn "Exception occurred during reader method compilation."
            logger.warn "Maybe #{attr_name} is not a valid Ruby identifier?"
            logger.warn "#{err.message}"
          end
        end
      end
    end
  end
end
