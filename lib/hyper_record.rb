require File.dirname(__FILE__) + '/hypertable/thrift_client'
require File.dirname(__FILE__) + '/active_record/connection_adapters/hypertable_adapter'
require File.dirname(__FILE__) + '/associations/hyper_has_many_association_extension'
require File.dirname(__FILE__) + '/associations/hyper_has_and_belongs_to_many_association_extension'

module ActiveRecord
  class Base
    def self.inherited(child) #:nodoc:
      return if child == ActiveRecord::HyperBase

      @@subclasses[self] ||= []
      @@subclasses[self] << child
      super
    end
  end

  class HyperBase < Base
    cattr_accessor :log_calls 

    # All records must include a ROW key
    validates_presence_of :ROW

    BILLION = 1_000_000_000.0

    ROW_KEY_OFFSET = 0
    COLUMN_FAMILY_OFFSET = 1
    COLUMN_QUALIFIER_OFFSET = 2
    VALUE_OFFSET = 3
    TIMESTAMP_OFFSET = 4

    def initialize(attrs={})
      super(attrs)
      self.ROW = attrs[:ROW] if attrs[:ROW] && attrs[:ROW]
    end

    # Instance Methods
    def update(mutator=nil)
      write_quoted_attributes(attributes_with_quotes(false, false),
        self.class.table_name, mutator)
      true
    end

    def create(mutator=nil)
      write_quoted_attributes(attributes_with_quotes(false, false), 
        self.class.table_name, mutator)
      @new_record = false
      self.attributes[self.class.primary_key]
    end

    def save_with_mutator(mutator)
      create_or_update_with_mutator(mutator)
    end

    def save_with_mutator!(mutator)
      create_or_update_with_mutator(mutator) || raise(RecordNotSaved)
    end

    def create_or_update_with_mutator(mutator)
      raise ReadOnlyRecord if readonly?
      result = new_record? ? create(mutator) : update(mutator)
      result != false
    end

    def destroy
      # check for associations and delete association cells as necessary
      for reflection_key in self.class.reflections.keys
        case self.class.reflections[reflection_key].macro
          when :has_and_belongs_to_many
            # remove all the association cells from the associated objects
            cells_to_delete = []

            for row_key in self.send(self.class.reflections[reflection_key].association_foreign_key).keys
              cells_to_delete << connection.cell_native_array(row_key, self.class.reflections[reflection_key].primary_key_name, self.ROW)
            end

            self.delete_cells(cells_to_delete, self.class.reflections[reflection_key].klass.table_name)
        end
      end

      self.class.connection.delete_rows(self.class.table_name, [self.ROW])
    end

    def increment(attribute, by=1)
      self[attribute] = self[attribute].to_i
      self[attribute] += by
      self
    end

    def increment!(attribute, by=1)
      increment(attribute, by)
      self.save
    end

    def decrement(attribute, by=1)
      increment(attribute, -by)
    end

    def decrement!(attribute, by=1)
      increment!(attribute, -by)
    end

    # Returns a copy of the attributes hash where all the values have been
    # safely quoted for insertion.  Translated qualified columns from a Hash
    # value in Ruby to a flat list of attributes.
    #
    # => {
    #  "ROW" => "page_1",
    #  "name" => "name",
    #  "url" => "http://www.icanhascheezburger.com"
    # }
    def attributes_with_quotes(include_primary_key = true, include_readonly_attributes = true)
      quoted = attributes.inject({}) do |quoted, (name, value)|
        if column = column_for_attribute(name)
          if column.is_a?(ConnectionAdapters::QualifiedColumn) and value.is_a?(Hash)
            value.keys.each{|k|
              quoted[self.class.connection.qualified_column_name(column.name, k)] = quote_value(value[k], column)
            }
          else
            quoted[name] = quote_value(value, column) unless !include_primary_key && column.primary
          end
        end
        quoted
      end
      include_readonly_attributes ? quoted : remove_readonly_attributes(quoted)
    end

    # Translates the output of attributes_with_quotes into an array of
    # cells suitable for writing into Hypertable (through the write_cells
    # method).  Data format is native array format for cells.
    # [
    #   ["row_key", "column_family", "column_qualifier", "value"],
    # ]
    def quoted_attributes_to_cells(quoted_attrs, table=self.class.table_name)
      cells = []
      pk = self.attributes[self.class.primary_key]
      quoted_attrs.keys.each{|key|
        name, qualifier = connection.hypertable_column_name(key, table).split(':', 2)
        cells << connection.cell_native_array(pk, name, qualifier, quoted_attrs[key])
      }
      cells
    end

    def write_quoted_attributes(quoted_attrs, table=self.class.table_name, mutator=nil)
      write_cells(quoted_attributes_to_cells(quoted_attrs, table), table, mutator)
    end

    # Write an array of cells to Hypertable
    def write_cells(cells, table=self.class.table_name, mutator=nil)
      if HyperBase.log_calls
        msg = [
          "Writing #{cells.length} cells to #{table} table",
          cells.map{|c| [c[0], c[1], c[2], c[3].to_s.first(20)].compact.join("\t")}
        ].join("\n")
        RAILS_DEFAULT_LOGGER.info(msg)
        # puts msg
      end

      connection.write_cells(table, cells, mutator)
    end

    # Delete an array of cells from Hypertable
    # cells is an array of cell keys [["row", "column"], ...]
    def delete_cells(cells, table=self.class.table_name)
      if HyperBase.log_calls
        msg = [
          "Deleting #{cells.length} cells from #{table} table",
          cells.map{|c| [ c[0], c[1] ].compact.join("\t")}
        ].join("\n")
        RAILS_DEFAULT_LOGGER.info(msg)
        # puts msg
      end

      connection.delete_cells(table, cells)
    end

    # Delete an array of rows from Hypertable
    # rows is an array of row keys ["row1", "row2", ...]
    def delete_rows(row_keys, table=self.class.table_name)
      connection.delete_rows(table, cells)
    end

    # Class Methods
    class << self
      def abstract_class?
        self == ActiveRecord::HyperBase
      end

      def exists?(id_or_conditions)
        case id_or_conditions
          when Fixnum, String
            !find(:first, :row_keys => [id_or_conditions]).nil?
          when Hash
            !find(:first, :conditions => id_or_conditions).nil?
          else
            raise "only Fixnum, String and Hash arguments supported"
        end
      end

      def delete(*ids)
        self.connection.delete_rows(table_name, ids.flatten)
      end

      def find(*args)
        options = args.extract_options!

        case args.first
          when :first then find_initial(options)
          when :all   then find_by_options(options)
          else             find_from_ids(args, options)
        end
      end

      def find_initial(options)
        options.update(:limit => 1)
        find_by_options(options).first
      end

      def find_by_options(options)
        options[:table_name] ||= table_name
        options[:columns] ||= columns

        # Don't request the ROW key explicitly, it always comes back
        options[:select] ||= qualified_column_names_without_row_key.map{|c| connection.hypertable_column_name(c, table_name)}

        if HyperBase.log_calls
          msg = [ "Select" ]
          for key in options.keys
            case key
              when :columns
                msg << "  columns\t#{options[:columns].map{|c| c.name}.join(',')}"
              else
                msg << "  #{key}\t#{options[key]}"
            end
          end
          msg << "Returned #{cell_count} cells"

          RAILS_DEFAULT_LOGGER.info(msg)
          # puts msg
        end

        rows = []
        current_row = {}

        cells = connection.execute_with_options(options)

        # Cells are guaranteed to come back in row key order, so assemble
        # a row by iterating over each cell and checking to see if the row key
        # has changed.  If it has, then the row is complete and needs to be
        # instantiated before processing the next cell.
        cells.each_with_index do |cell, i|
          current_row['ROW'] = cell[ROW_KEY_OFFSET]

          family = connection.rubify_column_name(cell[COLUMN_FAMILY_OFFSET])

          if !cell[COLUMN_QUALIFIER_OFFSET].blank?
            current_row[family] ||= {}
            current_row[family][cell[COLUMN_QUALIFIER_OFFSET]] = cell[VALUE_OFFSET]
          else
            current_row[family] = cell[VALUE_OFFSET]
          end

          # Instantiate the row if we've processed all cells for the row
          next_index = i + 1

          # Check to see if next cell has different row key or if we're at
          # the end of the cell stream.
          if (cells[next_index] and cells[next_index][ROW_KEY_OFFSET] != current_row['ROW']) or next_index >= cells.length
            # Make sure that the resulting object has attributes for all
            # columns - even ones that aren't in the response (due to limited
            # select)
            for col in column_families_without_row_key
              if !current_row.has_key?(col.name)
                if col.is_a?(ActiveRecord::ConnectionAdapters::QualifiedColumn)
                  current_row[col.name] = {}
                else
                  current_row[col.name] = nil
                end
              end
            end

            rows << instantiate(current_row)
            current_row = {}
          end
        end

        rows
      end

      def find_from_ids(ids, options)
        expects_array = ids.first.kind_of?(Array)
        return ids.first if expects_array && ids.first.empty?
        ids = ids.flatten.compact.uniq

        case ids.size
          when 0
            raise RecordNotFound, "Couldn't find #{name} without an ID"
          when 1
            result = find_one(ids.first, options)
            expects_array ? [ result ] : result
          else
            find_some(ids, options)
        end
      end

      def find_one(id, options)
        return nil if id.blank?

        options[:row_keys] = [id.to_s]

        if result = find_initial(options)
          result
        else
          raise ::ActiveRecord::RecordNotFound, "Couldn't find #{name} with ID=#{id}"
        end
      end

      def find_some(ids, options)
        options[:row_keys] = [ids.map{|i| i.to_s}]
        find_by_options(options)
      end

      def table_exists?(name=table_name)
        connection.tables.include?(name)
      end

      def drop_table
        connection.drop_table(table_name) if table_exists?
      end

      # Returns the primary key field for a table.  In Hypertable, a single
      # row key exists for each row.  The row key is referred to as ROW
      # in HQL, so we'll refer to it the same way here.
      def primary_key
        "ROW"
      end

      # Returns array of column objects for table associated with this class.
      def columns
        unless @columns
          @columns = connection.columns(table_name, "#{name} Columns")
          @qualified_columns ||= []
          @qualified_columns.each{|qc|
            # Remove the column family from the column list
            @columns = @columns.reject{|c| c.name == qc[:column_name].to_s}
            connection.remove_column_from_name_map(table_name, qc[:column_name].to_s)

            # Add the new qualified column family to the column list
            @columns << connection.add_qualified_column(table_name, qc[:column_name].to_s, qc[:qualifiers])
          }
          @columns.each {|column| column.primary = column.name == primary_key}
        end
        @columns
      end

      def qualified?(column_name)
        @qualified_columns.map{|c| c[:column_name]}.include?(column_name.to_sym)
      end

      def quoted_column_names(attributes=attributes_with_quotes)
        attributes.keys.collect do |column_name|
          self.class.connection.quote_column_name_for_table(column_name, table_name)
        end
      end

      def column_families_without_row_key
        columns[1,columns.length]
      end

      def qualified_column_names_without_row_key
        cols = column_families_without_row_key.map{|c| c.name}
        for qc in @qualified_columns
          cols.delete(qc[:column_name].to_s)
          for qualifier in qc[:qualifiers]
            cols << "#{qc[:column_name]}:#{qualifier}"
          end
        end
        cols
      end

      # qualified_column :misc, :qualifiers => [:name, :url]
      attr_accessor :qualified_columns
      def qualified_column(*attrs)
        @qualified_columns ||= []
        name = attrs.shift

        qualifiers = attrs.shift
        qualifiers = qualifiers.symbolize_keys[:qualifiers] if qualifiers
        @qualified_columns << {
          :column_name => name,
          :qualifiers => qualifiers || []
        }
      end

      # Mutator methods - passed through straight to the Hypertable Adapter.

      # Return an open mutator on this table.
      def open_mutator
        self.connection.open_mutator(table_name)
      end

      def close_mutator(mutator, flush=true)
        self.connection.close_mutator(mutator, flush)
      end

      def flush_mutator(mutator)
        self.connection.flush_mutator(mutator)
      end
    end
  end
end
