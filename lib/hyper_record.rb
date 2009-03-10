require File.dirname(__FILE__) + '/hypertable/thrift_client'
require File.dirname(__FILE__) + '/active_record/connection_adapters/hypertable_adapter'
require File.dirname(__FILE__) + '/active_record/qualified_column_attribute_handler.rb'
require File.dirname(__FILE__) + '/active_record/hyper_attribute_methods.rb'
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
    include ActiveRecord::HyperAttributeMethods

    # All records must include a ROW key
    validates_presence_of :ROW

    BILLION = 1_000_000_000.0

    def initialize(attrs=nil)
      super(attrs)
      self.ROW = attrs[:ROW] if attrs && attrs[:ROW]
    end

    # Instance Methods
    def update
      write_quoted_attributes(attributes_with_quotes(false, false))
      true
    end

    def create
      write_quoted_attributes(attributes_with_quotes(false, false))
      @new_record = false
      self.attributes[self.class.primary_key]
    end

    def destroy
      # check for associations and delete association cells as necessary
      for reflection_key in self.class.reflections.keys
        case self.class.reflections[reflection_key].macro
          when :has_and_belongs_to_many
            # remove all the association cells from the associated objects
            cells_to_delete = []

            for row_key in self.send(self.class.reflections[reflection_key].association_foreign_key).keys
              cells_to_delete << [row_key, self.class.connection.qualified_column_name(self.class.reflections[reflection_key].primary_key_name, self.ROW)]
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
    # => {
    #  "name"=>[["page_1", "name", "LOLcats and more", 1236655487367842001, -9223372036854775806, 255]],
    #  "url"=>[["page_1", "url", "http://www.icanhascheezburger.com", 1236655487367842002, -9223372036854775806, 255]]
    # }
    #
    # Each attribute is an array of values, where the most recent revision
    # of a cell occupies the first position in the array.
    def attributes_with_quotes(include_primary_key = true, include_readonly_attributes = true)
      quoted = {}
      for name in @attributes.keys
        cell_set = @attributes[name]
        # QualifiedColumnAttributeHandler are placeholders that proxy the
        # actual cell_set into other attribute keys
        # Skip over the ROW key entry
        next if name == 'ROW'
        next if cell_set.is_a?(QualifiedColumnAttributeHandler)

        quoted[name] ||= []
        for cell in cell_set
          quoted[name] << connection.cell_hash_to_array(self.ROW, cell)
        end
      end

      include_readonly_attributes ? quoted : remove_readonly_attributes(quoted)
    end

    # Translates the output of attributes_with_quotes into an array of
    # cells suitable for writing into Hypertable (through the write_cells
    # method).  Data format is native array format for cells.
    # => [["page_1", "name", "LOLcats and more", 1236655630845820001, -9223372036854775806, 255], ["page_1", "url", "http://www.icanhascheezburger.com", 1236655630845820002, -9223372036854775806, 255]]
    def quoted_attributes_to_cells(quoted_attrs, table=self.class.table_name)
      cells = []
      pk = self.attributes[self.class.primary_key]
      quoted_attrs.keys.each{|key| cells += quoted_attrs[key] }
      cells
    end

    def write_quoted_attributes(quoted_attrs, table=self.class.table_name)
      write_cells(quoted_attributes_to_cells(quoted_attrs, table))
    end

    # Write an array of cells to Hypertable
    def write_cells(cells, table=self.class.table_name)
      if ENV['RAILS_ENV'] != 'production'
        msg = [
          "Writing #{cells.length} cells to #{table} table",
          cells.map{|c| [
            c[0], c[1], c[2].to_s.first(20), c[3], c[4], c[5]
          ].compact.join("\t")}
        ].join("\n")
        RAILS_DEFAULT_LOGGER.info(msg)
        # puts msg
      end
      connection.write_cells(table, cells)
    end

    # Delete an array of cells from Hypertable
    # cells is an array of cell keys [["row", "column"], ...]
    def delete_cells(cells, table=self.class.table_name)
      if ENV['RAILS_ENV'] != 'production'
        msg = [
          "Deleting #{cells.length} cells from #{table} table",
          cells.map{|c| [
            c[0], c[1]
          ].compact.join("\t")}
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

    # Attribute methods

    # Initializes the attributes array with keys matching the columns from
    # the linked table and the values matching the corresponding default
    # value of that column, so that a new instance, or one populated from a
    # passed-in Hash, still has all the attributes that instances loaded from
    # the database would.
    #
    # Response is a Hash of default attribute values - normally a blank string 
    # for scalar columns and a Hash (actually an instance of
    # QualifiedColumnAttributeHandler for qualified columns)
    # => {"name"=>"", "url"=>"", "misc"=>{}, "misc2"=>{}}
    def attributes_from_column_definition
      self.class.columns.inject({}) do |attributes, column|
        if column.is_a?(ConnectionAdapters::QualifiedColumn)
          attributes[column.name] = QualifiedColumnAttributeHandler.new(self, column.name, column.default)
        else
          attributes[column.name] = column.default unless column.name == self.class.primary_key
        end
        attributes
      end
    end

    def raw_cells
      @attributes
    end

    # Reloads the attributes of this object from the database.
    # The optional options argument is passed to find when reloading so you
    # may do e.g. record.reload(:lock => true) to reload the same record with
    # an exclusive row lock.
    def reload(options = nil)
      clear_aggregation_cache
      clear_association_cache
      pk = self.attributes[self.class.primary_key]
      @attributes.clear
      self.class.find(pk, options).instance_variable_get('@attributes').each_pair{|key, value| @attributes[key] = value}
      @attributes_cache = {}
      self
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

        rows = Hash.new{|h,k| h[k] = []}

        cell_count = 0
        for cell in connection.execute_with_options(options)
          rows[cell['row_key']] << cell
          cell_count += 1
        end

        if ENV['RAILS_ENV'] != 'production'
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

        objects = []
        rows.each do |row_key, row|
          row_with_mapped_column_names = { 'ROW' => row.first['row_key'] }

          for cell in row
            if cell['column_qualifier']
              family = connection.rubify_column_name(cell['column_family'])
              row_with_mapped_column_names[family] ||= {}
              row_with_mapped_column_names[family][cell['column_qualifier']] = cell['value']
            else
              family = connection.rubify_column_name(cell['column_family'])
              row_with_mapped_column_names[family] = cell['value']
            end
          end

          # make sure that the resulting object has attributes for all
          # columns - even ones that aren't in the response (due to limited
          # select)
          for column in column_families_without_row_key
            if !row_with_mapped_column_names.has_key?(column.name)
              if column.is_a?(ActiveRecord::ConnectionAdapters::QualifiedColumn)
                row_with_mapped_column_names[column.name] = {}
              else
                row_with_mapped_column_names[column.name] = nil
              end
            end
          end

          object = instantiate(row_with_mapped_column_names, row)
          object.ROW = row.first['row_key']
          objects << object
        end

        objects
      end

      # Instantiate an AR object by invoking the superclass instantiate
      # method.  Then add the @attributes instance variable that allows access 
      # to cell metadata (revision, timestamp, etc.).  @attributes typically
      # contains scalar values (strings, integers, etc.) in regular
      # ActiveRecord.  In HyperRecord, each cell value is a Hash
      # that includes the metadata for the cell.
      def instantiate(record, cells)
        object = super(record)
        cells_by_column = Hash.new{|h,k| h[k] = []}
        cells.each{|c|
          key = [c['column_family'], c['column_qualifier']].compact.join(':')
          cells_by_column[key] << c
        }

        record.keys.each{|key|
          # Set up qualified column attribute handler proxies for each
          # qualified column.
          if (column = object.column_for_attribute(key)) && column.is_a?(ConnectionAdapters::QualifiedColumn)
            cells_by_column[key] = QualifiedColumnAttributeHandler.new(object, column.name, column.default) if cells_by_column[key].blank?
          else
            # Ensure that anything with a value in the instantiated object
            # has an entry in @attributes, otherwise "missing attribute"
            # exception is raised.
            cells_by_column[key] ||= nil
          end
        }

        object.instance_variable_set("@attributes", cells_by_column)
        object
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
        @qualified_columns.map{|qc| qc[:column_name]}.include?(column_name.to_sym)
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
    end
  end
end
