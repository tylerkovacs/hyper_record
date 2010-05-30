# For each supported data store, ActiveRecord has an adapter that implements 
# functionality specific to that store as well as providing metadata for 
# data held within the store. Features implemented by adapters typically 
# include connection handling, listings metadata (tables and schema), 
# statement execution (selects, writes, etc.), latency measurement, fixture 
# handling.
#
# This file implements the adapter for Hypertable used by ActiveRecord
# (HyperRecord).  The adapter communicates with Hypertable using the
# Thrift client API documented here:
# http://hypertable.org/thrift-api-ref/index.html
#
# Refer to the main hypertable site (http://hypertable.org/) for additional 
# information and documentation (http://hypertable.org/documentation.html)
# on Hypertable and the Thrift client API.

unless defined?(ActiveRecord::ConnectionAdapters::AbstractAdapter)
  # running into some situations where rails has already loaded this, without
  # require realizing it, and loading again is unsafe (alias_method_chain is a
  # great way to create infinite recursion loops)
  require 'active_record/connection_adapters/abstract_adapter'
end
require 'active_record/connection_adapters/qualified_column'
require 'active_record/connection_adapters/hyper_table_definition'

module ActiveRecord
  class Base
    # Include the thrift driver if one hasn't already been loaded
    def self.require_hypertable_thrift_client
      unless defined? Hypertable::ThriftClient
        gem 'hypertable-thrift-client'
        require_dependency 'thrift_client'
      end
    end

    # Establishes a connection to the Thrift Broker (which brokers requests
    # to Hypertable itself.  The connection details must appear in 
    # config/database.yml.  e.g.,
    # hypertable_dev:
    #  host: localhost
    #  port: 38088
    #  timeout: 20000
    #
    # Options:
    # * <tt>:host</tt> - Defaults to localhost
    # * <tt>:port</tt> - Defaults to 38088
    # * <tt>:timeout</tt> - Timeout for queries in milliseconds. Defaults to 20000
    def self.hypertable_connection(config)
      config = config.symbolize_keys
      require_hypertable_thrift_client

      raise "Hypertable config missing :host in database.yml" if !config[:host]

      config[:host] ||= 'localhost'
      config[:port] ||= 38088
      config[:timeout] ||= 20000

      connection = Hypertable::ThriftClient.new(config[:host], config[:port], 
        config[:timeout])

      ConnectionAdapters::HypertableAdapter.new(connection, logger, config)
    end
  end

  module ConnectionAdapters
    class HypertableAdapter < AbstractAdapter
      # Following cattr_accessors are used to record and access query 
      # performance statistics.
      @@read_latency = 0.0
      @@write_latency = 0.0
      @@cells_read = 0
      cattr_accessor :read_latency, :write_latency, :cells_read

      # Used by retry_on_connection_error() to determine whether to retry
      @retry_on_failure = true
      attr_accessor :retry_on_failure

      def initialize(connection, logger, config)
        super(connection, logger)
        @config = config
        @hypertable_column_names = {}
      end

      def raw_thrift_client(&block)
        t1 = Time.now
        results = Hypertable.with_thrift_client(@config[:host], @config[:port], 
          @config[:timeout], &block)
        @@read_latency += Time.now - t1
        results 
      end

      # Return the current set of performance statistics.  The application
      # can retrieve (and reset) these statistics after every query or
      # request for its own logging purposes.  
      def self.get_timing
        [@@read_latency, @@write_latency, @@cells_read]
      end

      # Reset performance metrics.
      def self.reset_timing
        @@read_latency = 0.0
        @@write_latency = 0.0
        @@cells_read = 0
      end

      def adapter_name
        'Hypertable'
      end

      def supports_migrations?
        true
      end

      # Hypertable only supports string types at the moment, so treat
      # all values as strings and leave it to the application to handle
      # types.
      def native_database_types
        {
          :string      => { :name => "varchar", :limit => 255 }
        }
      end

      def sanitize_conditions(options)
        case options[:conditions]
          when Hash
            # requires Hypertable API to support query by arbitrary cell value
            raise "HyperRecord does not support specifying conditions by Hash"
          when NilClass
            # do nothing
          else
            raise "Only hash conditions are supported"
        end
      end

      # Execute an HQL query against Hypertable and return the native 
      # HqlResult object that comes back from the Thrift client API.
      def execute(hql, name=nil)
        log(hql, name) {
          retry_on_connection_error { @connection.hql_query(hql) }
        }
      end

      # Execute a query against Hypertable and return the matching cells.
      # The query parameters are denoted in an options hash, which is 
      # converted to a "scan spec" by convert_options_to_scan_spec.
      # A "scan spec" is the mechanism used to specify query parameters
      # (e.g., the columns to retrieve, the number of rows to retrieve, etc.)
      # to Hypertable.
      def execute_with_options(options)
        scan_spec = convert_options_to_scan_spec(options)
        t1 = Time.now

        # Use native array method (get_cells_as_arrays) for cell retrieval - 
        # much faster than get_cells that returns Hypertable::ThriftGen::Cell
        # objects.
        # [
        #   ["page_1", "name", "", "LOLcats and more", "1237331693147619001"], 
        #   ["page_1", "url", "", "http://...", "1237331693147619002"]
        # ]
        cells = retry_on_connection_error {
          @connection.get_cells_as_arrays(options[:table_name], scan_spec)
        }

        # Capture performance metrics
        @@read_latency += Time.now - t1
        @@cells_read += cells.length

        cells
      end

      # Convert an options hash to a scan spec.  A scan spec is native 
      # representation of the query parameters that must be sent to 
      # Hypertable.
      # http://hypertable.org/thrift-api-ref/Client.html#Struct_ScanSpec
      def convert_options_to_scan_spec(options={})
        sanitize_conditions(options)

        # Rows can be specified using a number of different options:
        # :row_keys => [row_key_1, row_key_2, ...]
        # :start_row and :end_row
        # :row_intervals => [[start_1, end_1], [start_2, end_2]]
        row_intervals = []

        options[:start_inclusive] = options.has_key?(:start_inclusive) ? options[:start_inclusive] : true
        options[:end_inclusive] = options.has_key?(:end_inclusive) ? options[:end_inclusive] : true

        if options[:row_keys]
          options[:row_keys].flatten.each do |rk|
            row_intervals << [rk, rk]
          end
        elsif options[:row_intervals]
          options[:row_intervals].each do |ri|
            row_intervals << [ri.first, ri.last]
          end
        elsif options[:start_row]
          raise "missing :end_row" if !options[:end_row]
          row_intervals << [options[:start_row], options[:end_row]]
        end

        # Add each row interval to the scan spec
        options[:row_intervals] = row_intervals.map do |row_interval|
          ri = Hypertable::ThriftGen::RowInterval.new
          ri.start_row = row_interval.first
          ri.start_inclusive = options[:start_inclusive]
          ri.end_row = row_interval.last
          ri.end_inclusive = options[:end_inclusive]
          ri
        end

        scan_spec = Hypertable::ThriftGen::ScanSpec.new

        # Hypertable can store multiple revisions for each cell but this
        # feature does not map into an ORM very well.  By default, just 
        # retrieve the latest revision of each cell.  Since this is most 
        # common config when using HyperRecord, tables should be defined 
        # with MAX_VERSIONS=1 at creation time to save space and reduce 
        # query time.
        options[:revs] ||= 1

        # Most of the time, we're not interested in cells that have been 
        # marked deleted but have not actually been deleted yet.
        options[:return_deletes] ||= false

        for key in options.keys
          case key.to_sym
            when :row_intervals
              scan_spec.row_intervals = options[key]
            when :cell_intervals
              scan_spec.cell_intervals = options[key]
            when :start_time
              scan_spec.start_time = options[key]
            when :end_time
              scan_spec.end_time = options[key]
            when :limit
              scan_spec.row_limit = options[key]
            when :revs
              scan_spec.revs = options[key]
            when :return_deletes
              scan_spec.return_deletes = options[key]
            when :select
              # Columns listed here can only be column families (not
              # column qualifiers) at this time.
              requested_columns = if options[key].is_a?(String)
                requested_columns_from_string(options[key])
              elsif options[key].is_a?(Symbol)
                requested_columns_from_string(options[key].to_s)
              elsif options[key].is_a?(Array)
                 options[key].map{|k| k.to_s}
              else
                options[key]
              end

              scan_spec.columns = requested_columns.map do |column|
                status, family, qualifier = is_qualified_column_name?(column)
                family
              end.uniq
            when :table_name, :start_row, :end_row, :start_inclusive, :end_inclusive, :select, :columns, :row_keys, :conditions, :include, :readonly, :scan_spec, :instantiate_only_requested_columns
              # ignore
            else
              raise "Unrecognized scan spec option: #{key}"
          end
        end

        scan_spec
      end

      def requested_columns_from_string(s)
        if s == '*'
          []
        else
          s.split(',').map{|s| s.strip}
        end
      end

      # Exceptions generated by Thrift IDL do not set a message.
      # This causes a lot of problems for Rails which expects a String
      # value and throws exception when it encounters NilClass.
      # Unfortunately, you cannot assign a message to exceptions so define
      # a singleton to accomplish same goal.
      def handle_thrift_exceptions_with_missing_message
        begin
          yield
        rescue Exception => err
          if !err.message
            if err.respond_to?("message=")
              err.message = err.what || ''
            else
              def err.message
                self.what || ''
              end
            end
          end

          raise err
        end
      end

      # Attempt to reconnect to the Thrift Broker once before aborting.
      # This ensures graceful recovery in the case that the Thrift Broker
      # goes down and then comes back up.
      def retry_on_connection_error
        @retry_on_failure = true
        begin
          handle_thrift_exceptions_with_missing_message { yield }
        rescue Thrift::TransportException, IOError, Thrift::ApplicationException, Thrift::ProtocolException => err
          if @retry_on_failure
            @retry_on_failure = false
            @connection.close
            @connection.open
            retry
          else
            raise err
          end
        end
      end

      # Column Operations

      # Returns array of column objects for the table associated with this 
      # class.  Hypertable allows columns to include dashes in the name.  
      # This doesn't play well with Ruby (can't have dashes in method names), 
      # so we maintain a mapping of original column names to Ruby-safe 
      # names.
      def columns(table_name, name = nil)
        # Each table always has a row key called 'ROW'
        columns = [ Column.new('ROW', '') ]

        schema = describe_table(table_name)
        doc = REXML::Document.new(schema)
        column_families = doc.each_element('Schema/AccessGroup/ColumnFamily') { |cf| cf }

        @hypertable_column_names[table_name] ||= {}
        for cf in column_families
          # Columns are lazily-deleted in Hypertable so still may show up
          # in describe table output.  Ignore.
          deleted = cf.elements['deleted'].text
          next if deleted == 'true'

          column_name = cf.elements['Name'].text
          rubified_name = rubify_column_name(column_name)
          @hypertable_column_names[table_name][rubified_name] = column_name
          columns << new_column(rubified_name, '')
        end

        columns
      end

      def remove_column_from_name_map(table_name, name)
        @hypertable_column_names[table_name].delete(rubify_column_name(name))
      end

      def add_column_to_name_map(table_name, name)
        @hypertable_column_names[table_name][rubify_column_name(name)] = name
      end

      def add_qualified_column(table_name, column_family, qualifiers=[], default='', sql_type=nil, null=true)
        qc = QualifiedColumn.new(column_family, default, sql_type, null)
        qc.qualifiers = qualifiers
        qualifiers.each{|q| add_column_to_name_map(table_name, qualified_column_name(column_family, q))}
        qc
      end

      def new_column(column_name, default_value='')
        Column.new(rubify_column_name(column_name), default_value)
      end

      def qualified_column_name(column_family, qualifier=nil)
        [column_family, qualifier].compact.join(':')
      end

      def rubify_column_name(column_name)
        column_name.to_s.gsub(/-+/, '_')
      end

      def is_qualified_column_name?(column_name)
        column_family, qualifier = column_name.split(':', 2)
        if qualifier
          [true, column_family, qualifier]
        else
          [false, column_name, nil]
        end
      end

      # Schema alterations

      def rename_column(table_name, column_name, new_column_name)
        raise "rename_column operation not supported by Hypertable."
      end

      def change_column(table_name, column_name, new_column_name)
        raise "change_column operation not supported by Hypertable."
      end

      # Translate "sexy" ActiveRecord::Migration syntax to an HQL
      # CREATE TABLE statement.
      def create_table_hql(table_name, options={}, &block)
        table_definition = HyperTableDefinition.new(self)

        yield table_definition

        if options[:force] && table_exists?(table_name)
          drop_table(table_name, options)
        end

        create_sql = [ "CREATE TABLE #{quote_table_name(table_name)} (" ]
        column_sql = []
        for col in table_definition.columns
          column_sql << [
            quote_table_name(col.name),
            col.max_versions ? "MAX_VERSIONS=#{col.max_versions}" : ''
          ].join(' ')
        end
        create_sql << column_sql.join(', ')

        create_sql << ") #{options[:options]}"
        create_sql.join(' ').strip
      end

      def create_table(table_name, options={}, &block)
        execute(create_table_hql(table_name, options, &block))
      end

      def drop_table(table_name, options = {})
        retry_on_connection_error {
          @connection.drop_table(table_name, options[:if_exists] || false)
        }
      end

      def rename_table(table_name, options = {})
        raise "rename_table operation not supported by Hypertable."
      end

      def change_column_default(table_name, column_name, default)
        raise "change_column_default operation not supported by Hypertable."
      end

      def change_column_null(table_name, column_name, null, default = nil)
        raise "change_column_null operation not supported by Hypertable."
      end

      def add_column(table_name, column_name, type=:string, options = {})
        hql = [ "ALTER TABLE #{quote_table_name(table_name)} ADD (" ]
        hql << quote_column_name(column_name)
        hql << "MAX_VERSIONS=#{options[:max_versions]}" if !options[:max_versions].blank?
        hql << ")"
        execute(hql.join(' '))
      end

      def add_column_options!(hql, options)
        hql << " MAX_VERSIONS =1 #{quote(options[:default], options[:column])}" if options_include_default?(options)
        # must explicitly check for :null to allow change_column to work on migrations
        if options[:null] == false
          hql << " NOT NULL"
        end
      end

      def remove_column(table_name, *column_names)
        column_names.flatten.each do |column_name|
          execute "ALTER TABLE #{quote_table_name(table_name)} DROP(#{quote_column_name(column_name)})"
        end
      end
      alias :remove_columns :remove_column

      def quote(value, column = nil)
        case value
          when NilClass then ''
          when String then value
          else super(value, column)
        end
      end

      def quote_column_name(name)
        "'#{name}'"
      end

      def quote_column_name_for_table(name, table_name)
        quote_column_name(hypertable_column_name(name, table_name))
      end

      def hypertable_column_name(name, table_name, declared_columns_only=false)
        begin
          columns(table_name) if @hypertable_column_names[table_name].blank?
          n = @hypertable_column_names[table_name][name]
          n ||= name if !declared_columns_only
          n
        rescue Exception => err
          raise [
            "hypertable_column_name exception",
            err.message,
            "table: #{table_name}",
            "column: #{name}",
            "@htcn: #{pp @hypertable_column_names}"
          ].join("\n")
        end
      end

      # Return an XML document describing the table named in the first
      # argument.  Output is equivalent to that returned by the DESCRIBE
      # TABLE command available in the Hypertable CLI.
      # <Schema generation="2">
      #   <AccessGroup name="default">
      #     <ColumnFamily id="1">
      #       <Generation>1</Generation>
      #       <Name>date</Name>
      #       <deleted>false</deleted>
      #     </ColumnFamily>
      #   </AccessGroup>
      # </Schema>
      def describe_table(table_name)
        retry_on_connection_error {
          @connection.get_schema(table_name)
        }
      end

      # Returns an array of tables available in the current Hypertable 
      # instance.
      def tables(name=nil)
        retry_on_connection_error {
          @connection.get_tables
        }
      end

      # Write an array of cells to the named table.  By default, write_cells
      # will open and close a mutator for this operation.  Closing the
      # mutator flushes the data, which guarantees is it is stored in 
      # Hypertable before the call returns.  This also slows down the 
      # operation, so if you're doing lots of writes and want to manage
      # mutator flushes at the application layer then you can pass in a
      # mutator as argument.  Mutators can be created with the open_mutator
      # method.  In the near future (Summer 2009), Hypertable will provide
      # a periodic mutator that automatically flushes at specific intervals.
      def write_cells(table_name, cells, options={})
        return if cells.blank?

        mutator = options[:mutator]
        flags = options[:flags]
        flush_interval = options[:flush_interval]

        retry_on_connection_error {
          local_mutator_created = !mutator

          begin
            t1 = Time.now
            mutator ||= open_mutator(table_name, flags, flush_interval)
            if options[:asynchronous_write]
              mutate_spec = Hypertable::ThriftGen::MutateSpec.new
              mutate_spec.appname = 'hyper_record'
              mutate_spec.flush_interval = 1000
              mutate_spec.flags = 2
              @connection.put_cells_as_arrays(table_name, mutate_spec, cells)
            else
              @connection.set_cells_as_arrays(mutator, cells)
            end
          ensure
            if local_mutator_created && mutator
              close_mutator(mutator)
              mutator = nil
            end
            @@write_latency += Time.now - t1
          end
        }
      end

      # Return a Hypertable::ThriftGen::Cell object from a cell passed in
      # as an array of format: [row_key, column_name, value]
      # Hypertable::ThriftGen::Cell objects are required when setting a flag
      # on write - used by special operations (e.g,. delete )
      def thrift_cell_from_native_array(array)
        cell = Hypertable::ThriftGen::Cell.new
        cell.key = Hypertable::ThriftGen::Key.new
        cell.key.row = array[0]
        cell.key.column_family = array[1]
        cell.key.column_qualifier = array[2] if !array[2].blank?
        cell.value = array[3] if array[3]
        cell.key.timestamp = array[4] if array[4]
        cell
      end

      # Create native array format for cell.  Most HyperRecord operations
      # deal with cells in native array format since operations on an
      # array are much faster than operations on Hypertable::ThriftGen::Cell 
      # objects.
      # ["row_key", "column_family", "column_qualifier", "value"],
      def cell_native_array(row_key, column_family, column_qualifier, value=nil, timestamp=nil)
        [
          row_key.to_s,
          column_family.to_s,
          column_qualifier.to_s,
          value.to_s
        ].map do |s| 
          s.respond_to?(:force_encoding) ? s.force_encoding('ascii-8bit') : s 
        end
      end

      # Delete cells from a table.
      def delete_cells(table_name, cells)
        t1 = Time.now

        retry_on_connection_error {
          @connection.with_mutator(table_name) do |mutator|
            thrift_cells = cells.map{|c|
              cell = thrift_cell_from_native_array(c)
              cell.flag = Hypertable::ThriftGen::CellFlag::DELETE_CELL
              cell
            }
            @connection.set_cells(mutator, thrift_cells)
          end
        }

        @@write_latency += Time.now - t1
      end

      # Delete rows from a table.
      def delete_rows(table_name, row_keys)
        t1 = Time.now
        cells = row_keys.map do |row_key|
          cell = Hypertable::ThriftGen::Cell.new
          cell.key = Hypertable::ThriftGen::Key.new
          cell.key.row = row_key
          cell.key.flag = Hypertable::ThriftGen::CellFlag::DELETE_ROW
          cell
        end

        retry_on_connection_error {
          @connection.with_mutator(table_name) do |mutator|
            @connection.set_cells(mutator, cells)
          end
        }

        @@write_latency += Time.now - t1
      end

      # Insert a test fixture into a table.
      def insert_fixture(fixture, table_name)
        fixture_hash = fixture.to_hash
        timestamp = fixture_hash.delete('timestamp')
        row_key = fixture_hash.delete('ROW')
        cells = []
        fixture_hash.keys.each do |k|
          column_name, column_family = k.split(':', 2)
          cells << cell_native_array(row_key, column_name, column_family, fixture_hash[k], timestamp)
        end
        write_cells(table_name, cells)
      end

      # Mutator methods

      def open_mutator(table_name, flags=0, flush_interval=0)
        @connection.open_mutator(table_name, flags, flush_interval)
      end

      # Flush is always called in a mutator's destructor due to recent
      # no_log_sync changes.  Adding an explicit flush here just adds
      # one round trip for an extra flush call, so change the default to
      # flush=0.  Consider removing this argument and always sending 0.
      def close_mutator(mutator, flush=0)
        @connection.close_mutator(mutator, flush)
      end

      def flush_mutator(mutator)
        @connection.flush_mutator(mutator)
      end

      # Scanner methods

      def open_scanner(table_name, scan_spec)
        @connection.open_scanner(table_name, scan_spec, true)
      end

      def close_scanner(scanner)
        @connection.close_scanner(scanner)
      end

      def with_scanner(table_name, scan_spec, &block)
        @connection.with_scanner(table_name, scan_spec, &block)
      end

      # Iterator methods

      def each_cell(scanner, &block)
        @connection.each_cell(scanner, &block)
      end

      def each_cell_as_arrays(scanner, &block)
        @connection.each_cell_as_arrays(scanner, &block)
      end

      def each_row(scanner, &block)
        @connection.each_row(scanner, &block)
      end

      def each_row_as_arrays(scanner, &block)
        @connection.each_row_as_arrays(scanner, &block)
      end
    end
  end
end
