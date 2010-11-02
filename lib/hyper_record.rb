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

    # Jonas Fix for "Can't dup NilClass" when dup'ing default_scoping
    # The default value for default_scoping in ActiveRecord::Base is []
    # Because it is defined in Base.rb with class_inheritable_accessor, 
    # HyperBase has it's own version that needs to be initialized.
    # TODO: in the future there may be other HyperRecord::Base class variables 
    # that are initialized in base.rb. They will cause problems, too
    self.default_scoping = self.superclass.default_scoping

    def initialize(attrs={})
      super(attrs)
      self.ROW = attrs[:ROW] if attrs[:ROW] && attrs[:ROW]
    end

    # Instance Methods
    def update(mutator=self.class.mutator)
      write_quoted_attributes(attributes_with_quotes(false, false),
        self.class.table_name, mutator)
      true
    end

    def create(mutator=self.class.mutator)
      write_quoted_attributes(attributes_with_quotes(false, false), 
        self.class.table_name, mutator)
      @new_record = false
      self.attributes[self.class.primary_key]
    end

    # Allows the save operation to be performed with a specific
    # mutator.  By default, a new mutator is opened, flushed and closed for
    # each save operation.  Write-heavy application may wish to manually
    # manage mutator flushes (which happens when the mutator is closed) at 
    # the application-layer in order to increase write throughput.
    #
    #   m = Page.open_mutator
    #
    #   p1 = Page.new({:ROW => 'created_with_mutator_1', :url => 'url_1'})
    #   p1.save_with_mutator!(m)
    #   p2 = Page.new({:ROW => 'created_with_mutator_2', :url => 'url_2'})
    #   p2.save_with_mutator!(m)
    #
    #   Page.close_mutator(m)
    # 
    # Future versions of hypertable will provide a mutator that automatically
    # periodically flushes.  This feature is expected by Summary 2009.  At
    # that time, manually managing the mutator at the 
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

    # Destroy an object.  Since Hypertable does not have foreign keys,
    # association cells must be removed manually.
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

    # Casts the attribute to an integer before performing the increment.  This
    # is necessary because Hypertable only supports integer types at the
    # moment.  The cast has the effect of initializing nil values (and most
    # string values) to zero.
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
    # safely quoted for insertion.  Translates qualified columns from a Hash
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

    def write_quoted_attributes(quoted_attrs, table=self.class.table_name, mutator=self.class.mutator)
      write_cells(quoted_attributes_to_cells(quoted_attrs, table), table, mutator)
    end

    # Write an array of cells to Hypertable
    def write_cells(cells, table=self.class.table_name, mutator=self.class.mutator)
      if HyperBase.log_calls
        msg = [
          "Writing #{cells.length} cells to #{table} table",
          cells.map{|c| [c[0], c[1], c[2], c[3].to_s.first(20)].compact.join("\t")}
        ].join("\n")
        RAILS_DEFAULT_LOGGER.info(msg)
        # puts msg
      end

      connection.write_cells(table, cells, {
        :mutator => mutator, 
        :flags => self.class.mutator_flags, 
        :flush_interval => self.class.mutator_flush_interval,
        :asynchronous_write => self.class.asynchronous_write
      })
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

      # Converts incoming finder options into a scan spec.  A scan spec
      # is an object used to describe query parameters (columns to retrieve,
      # number of rows to retrieve, row key ranges) for Hypertable queries.
      def find_to_scan_spec(*args)
        options = args.extract_options!
        options[:scan_spec] = true
        args << options
        find(*args)
      end

      # Returns a scanner object that allows you to iterate over the 
      # result set using the lower-level Thrift client APIs methods that
      # require a scanner object. e.g.,
      #
      # Page.find_with_scanner(:all, :limit => 1) do |scanner|
      #   Page.each_cell_as_arrays(scanner) do |cell|
      #     ...
      #   end
      # end
      #
      # See the Thrift Client API documentation for more detail.
      # http://hypertable.org/thrift-api-ref/index.html
      def find_with_scanner(*args, &block)
        scan_spec = find_to_scan_spec(*args)
        with_scanner(scan_spec, &block)
      end

      # Returns each row matching the finder options as a HyperRecord
      # object.  Each object is yielded to the caller so that large queries 
      # can be processed one object at a time without pulling the entire 
      # result set into memory.
      #
      # Page.find_each_row(:all) do |page|
      #   ...
      # end
      def find_each_row(*args)
        find_each_row_as_arrays(*args) do |row|
          yield convert_cells_to_instantiated_rows(row).first
        end
      end

      # Returns each row matching the finder options as an array of cells
      # in native array format.  Each row is yielded to the caller so that
      # large queries can be processed one row at a time without pulling
      # the entire result set into memory.
      #
      # Page.find_each_row(:all) do |page_as_array_of_cells|
      #   ...
      # end
      def find_each_row_as_arrays(*args)
        scan_spec = find_to_scan_spec(*args)
        with_scanner(scan_spec) do |scanner|
          row = []
          current_row_key = nil

          each_cell_as_arrays(scanner) do |cell|
            current_row_key ||= cell[ROW_KEY_OFFSET]

            if cell[ROW_KEY_OFFSET] == current_row_key
              row << cell
            else
              yield row
              row = [cell]
              current_row_key = cell[ROW_KEY_OFFSET]
            end
          end 

          yield row unless row.empty?
        end
      end

      # Each hypertable query requires some default options (e.g., table name)
      # that are set here if not specified in the query.
      def set_default_options(options)
        options[:table_name] ||= table_name
        options[:columns] ||= columns

        # Don't request the ROW key explicitly, it always comes back
        options[:select] ||= qualified_column_names_without_row_key.map{|c| 
          connection.hypertable_column_name(c, table_name)
        }
      end

      # Return the first record that matches the finder options.
      def find_initial(options)
        options.update(:limit => 1)

        if options[:scan_spec]
          find_by_options(options)
        else
          find_by_options(options).first
        end
      end

      # Return an array of records matching the finder options.
      def find_by_options(options)
        set_default_options(options)

        # If requested, instead of retrieving the matching cells from
        # Hypertable, simply return a scan spec that matches the finder
        # options.
        if options[:scan_spec]
          return connection.convert_options_to_scan_spec(options)
        end

        cells = connection.execute_with_options(options)

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

        convert_cells_to_instantiated_rows(cells, options)
      end

      # Converts cells that come back from Hypertable into hashes.  Each
      # hash represents a separate record (where each cell that has the same
      # row key is considered one record).
      def convert_cells_to_hashes(cells, options={})
        rows = []
        current_row = {}

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
              next if options[:instantiate_only_requested_columns] && !options[:select].include?(col.name)

              if !current_row.has_key?(col.name)
                if col.is_a?(ActiveRecord::ConnectionAdapters::QualifiedColumn)
                  current_row[col.name] = {}
                else
                  current_row[col.name] = nil
                end
              end
            end

            rows << current_row
            current_row = {}
          end
        end

        rows
      end

      def convert_cells_to_instantiated_rows(cells, options={})
        convert_cells_to_hashes(cells, options).map{|row| instantiate(row)}
      end

      # Return the records that match a specific HQL query.
      def find_by_hql(hql)
        hql_result = connection.execute(hql)
        cells_in_native_array_format = hql_result.cells.map do |c| 
          connection.cell_native_array(c.key.row, c.key.column_family, 
            c.key.column_qualifier, c.value)
        end
        convert_cells_to_instantiated_rows(cells_in_native_array_format)
      end
      alias :find_by_sql :find_by_hql

      # Return multiple records by row keys.
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

      # Return a single record identified by a row key.
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

      # row_key_attributes :regex => /_(\d{4}-\d{2}-\d{2}_\d{2}:\d{2})$/, :attribute_names => [:timestamp]
      attr_accessor :row_key_attributes
      def row_key_attributes(*attrs)
        symbolized_attrs = attrs.first.symbolize_keys
        regex = symbolized_attrs[:regex]
        names = symbolized_attrs[:attribute_names]
        
        names.each_with_index do |name, i|
          self.class_eval %{
            def #{name}
              @_row_key_attributes ||= {}       

              if !@_row_key_attributes['#{name}'] || self.ROW_changed?
                matches = self.ROW.to_s.match(#{regex.to_s.inspect})
                @_row_key_attributes['#{name}'] = if matches
                  (matches[#{i + 1}] || '')
                else
                  ''
                end
              end

              @_row_key_attributes['#{name}']
            end
          } 
        end

        if !names.blank?
          self.class_eval %{
            def self.assemble_row_key_from_attributes(attributes)
              %w(#{names.join(' ')}).map do |n| 
                attributes[n.to_sym]
              end.compact.join('_')
            end
          }
        end
      end

      attr_accessor :mutator, :mutator_flags, :mutator_flush_interval,
        :asynchronous_write

      def mutator_options(*attrs)
        symbolized_attrs = attrs.first.symbolize_keys
        @mutator_flags = symbolized_attrs[:flags].to_i
        @mutator_flush_interval = symbolized_attrs[:flush_interval].to_i
        @asynchronous_write = symbolized_attrs[:asynchronous_write]

        if symbolized_attrs[:persistent]
          @mutator = self.open_mutator(@mutator_flags, @mutator_flush_interval)
        end
      end

      # Mutator methods - passed through straight to the Hypertable Adapter.

      # Return an open mutator on this table.
      def open_mutator(flags=@mutator_flags.to_i, flush_interval=@mutator_flush_interval.to_i)
        self.connection.open_mutator(table_name, flags, flush_interval)
      end

      # As of Hypertable 0.9.2.5, flush is automatically performed on a
      # close_mutator call (so flush should default to 0).
      def close_mutator(mutator, flush=0)
        self.connection.close_mutator(mutator, flush)
      end

      def flush_mutator(mutator)
        self.connection.flush_mutator(mutator)
      end

      # Scanner methods
      def open_scanner(scan_spec)
        self.connection.open_scanner(self.table_name, scan_spec)
      end

      def close_scanner(scanner)
        self.connection.close_scanner(scanner)
      end

      def with_scanner(scan_spec, &block)
        self.connection.with_scanner(self.table_name, scan_spec, &block)
      end

      # Iterator methods
      def each_cell(scanner, &block)
        self.connection.each_cell(scanner, &block)
      end

      def each_cell_as_arrays(scanner, &block)
        self.connection.each_cell_as_arrays(scanner, &block)
      end

      def each_row(scanner, &block)
        self.connection.each_row(scanner, &block)
      end

      def each_row_as_arrays(scanner, &block)
        self.connection.each_row_as_arrays(scanner, &block)
      end

      def with_thrift_client(&block)
        self.connection.raw_thrift_client(&block)
      end
    end
  end
end
