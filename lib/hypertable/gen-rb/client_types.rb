#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#


module Hypertable
  module ThriftGen
        module CellFlag
          DELETE_ROW = 0
          DELETE_CF = 1
          DELETE_CELL = 2
          INSERT = 255
          VALUE_MAP = {0 => "DELETE_ROW", 1 => "DELETE_CF", 2 => "DELETE_CELL", 255 => "INSERT"}
          VALID_VALUES = Set.new([DELETE_ROW, DELETE_CF, DELETE_CELL, INSERT]).freeze
        end

        module MutatorFlag
          NO_LOG_SYNC = 1
          IGNORE_UNKNOWN_CFS = 2
          VALUE_MAP = {1 => "NO_LOG_SYNC", 2 => "IGNORE_UNKNOWN_CFS"}
          VALID_VALUES = Set.new([NO_LOG_SYNC, IGNORE_UNKNOWN_CFS]).freeze
        end

        # Specifies a range of rows
        # 
        # <dl>
        #   <dt>start_row</dt>
        #   <dd>The row to start scan with. Must not contain nulls (0x00)</dd>
        # 
        #   <dt>start_inclusive</dt>
        #   <dd>Whether the start row is included in the result (default: true)</dd>
        # 
        #   <dt>end_row</dt>
        #   <dd>The row to end scan with. Must not contain nulls</dd>
        # 
        #   <dt>end_inclusive</dt>
        #   <dd>Whether the end row is included in the result (default: true)</dd>
        # </dl>
        class RowInterval
          include ::Thrift::Struct, ::Thrift::Struct_Union
          START_ROW = 1
          START_INCLUSIVE = 2
          END_ROW = 3
          END_INCLUSIVE = 4

          FIELDS = {
            START_ROW => {:type => ::Thrift::Types::STRING, :name => 'start_row', :optional => true},
            START_INCLUSIVE => {:type => ::Thrift::Types::BOOL, :name => 'start_inclusive', :default => true, :optional => true},
            END_ROW => {:type => ::Thrift::Types::STRING, :name => 'end_row', :optional => true},
            END_INCLUSIVE => {:type => ::Thrift::Types::BOOL, :name => 'end_inclusive', :default => true, :optional => true}
          }

          def struct_fields; FIELDS; end

          def validate
          end

          ::Thrift::Struct.generate_accessors self
        end

        # Specifies a range of cells
        # 
        # <dl>
        #   <dt>start_row</dt>
        #   <dd>The row to start scan with. Must not contain nulls (0x00)</dd>
        # 
        #   <dt>start_column</dt>
        #   <dd>The column (prefix of column_family:column_qualifier) of the
        #   start row for the scan</dd>
        # 
        #   <dt>start_inclusive</dt>
        #   <dd>Whether the start row is included in the result (default: true)</dd>
        # 
        #   <dt>end_row</dt>
        #   <dd>The row to end scan with. Must not contain nulls</dd>
        # 
        #   <dt>end_column</dt>
        #   <dd>The column (prefix of column_family:column_qualifier) of the
        #   end row for the scan</dd>
        # 
        #   <dt>end_inclusive</dt>
        #   <dd>Whether the end row is included in the result (default: true)</dd>
        # </dl>
        class CellInterval
          include ::Thrift::Struct, ::Thrift::Struct_Union
          START_ROW = 1
          START_COLUMN = 2
          START_INCLUSIVE = 3
          END_ROW = 4
          END_COLUMN = 5
          END_INCLUSIVE = 6

          FIELDS = {
            START_ROW => {:type => ::Thrift::Types::STRING, :name => 'start_row', :optional => true},
            START_COLUMN => {:type => ::Thrift::Types::STRING, :name => 'start_column', :optional => true},
            START_INCLUSIVE => {:type => ::Thrift::Types::BOOL, :name => 'start_inclusive', :default => true, :optional => true},
            END_ROW => {:type => ::Thrift::Types::STRING, :name => 'end_row', :optional => true},
            END_COLUMN => {:type => ::Thrift::Types::STRING, :name => 'end_column', :optional => true},
            END_INCLUSIVE => {:type => ::Thrift::Types::BOOL, :name => 'end_inclusive', :default => true, :optional => true}
          }

          def struct_fields; FIELDS; end

          def validate
          end

          ::Thrift::Struct.generate_accessors self
        end

        # Specifies options for a scan
        # 
        # <dl>
        #   <dt>row_intervals</dt>
        #   <dd>A list of ranges of rows to scan. Mutually exclusive with
        #   cell_interval</dd>
        # 
        #   <dt>cell_intervals</dt>
        #   <dd>A list of ranges of cells to scan. Mutually exclusive with
        #   row_intervals</dd>
        # 
        #   <dt>return_deletes</dt>
        #   <dd>Indicates whether cells pending delete are returned</dd>
        # 
        #   <dt>revs</dt>
        #   <dd>Specifies max number of revisions of cells to return</dd>
        # 
        #   <dt>row_limit</dt>
        #   <dd>Specifies max number of rows to return</dd>
        # 
        #   <dt>start_time</dt>
        #   <dd>Specifies start time in nanoseconds since epoch for cells to
        #   return</dd>
        # 
        #   <dt>end_time</dt>
        #   <dd>Specifies end time in nanoseconds since epoch for cells to return</dd>
        # 
        #   <dt>columns</dt>
        #   <dd>Specifies the names of the columns to return</dd>
        # 
        #   <dt>cell_limit</dt>
        #   <dd>Specifies max number of cells to return per column family per row</dd>
        # </dl>
        class ScanSpec
          include ::Thrift::Struct, ::Thrift::Struct_Union
          ROW_INTERVALS = 1
          CELL_INTERVALS = 2
          RETURN_DELETES = 3
          REVS = 4
          ROW_LIMIT = 5
          START_TIME = 6
          END_TIME = 7
          COLUMNS = 8
          KEYS_ONLY = 9
          CELL_LIMIT = 10

          FIELDS = {
            ROW_INTERVALS => {:type => ::Thrift::Types::LIST, :name => 'row_intervals', :element => {:type => ::Thrift::Types::STRUCT, :class => Hypertable::ThriftGen::RowInterval}, :optional => true},
            CELL_INTERVALS => {:type => ::Thrift::Types::LIST, :name => 'cell_intervals', :element => {:type => ::Thrift::Types::STRUCT, :class => Hypertable::ThriftGen::CellInterval}, :optional => true},
            RETURN_DELETES => {:type => ::Thrift::Types::BOOL, :name => 'return_deletes', :default => false, :optional => true},
            REVS => {:type => ::Thrift::Types::I32, :name => 'revs', :default => 0, :optional => true},
            ROW_LIMIT => {:type => ::Thrift::Types::I32, :name => 'row_limit', :default => 0, :optional => true},
            START_TIME => {:type => ::Thrift::Types::I64, :name => 'start_time', :optional => true},
            END_TIME => {:type => ::Thrift::Types::I64, :name => 'end_time', :optional => true},
            COLUMNS => {:type => ::Thrift::Types::LIST, :name => 'columns', :element => {:type => ::Thrift::Types::STRING}, :optional => true},
            KEYS_ONLY => {:type => ::Thrift::Types::BOOL, :name => 'keys_only', :default => false, :optional => true},
            CELL_LIMIT => {:type => ::Thrift::Types::I32, :name => 'cell_limit', :default => 0, :optional => true}
          }

          def struct_fields; FIELDS; end

          def validate
          end

          ::Thrift::Struct.generate_accessors self
        end

        # Defines a cell key
        # 
        # <dl>
        #   <dt>row</dt>
        #   <dd>Specifies the row key. Note, it cannot contain null characters.
        #   If a row key is not specified in a return cell, it's assumed to
        #   be the same as the previous cell</dd>
        # 
        #   <dt>column_family</dt>
        #   <dd>Specifies the column family</dd>
        # 
        #   <dt>column_qualifier</dt>
        #   <dd>Specifies the column qualifier. A column family must be specified.</dd>
        # 
        #   <dt>timestamp</dt>
        #   <dd>Nanoseconds since epoch for the cell<dd>
        # 
        #   <dt>revision</dt>
        #   <dd>A 64-bit revision number for the cell</dd>
        # 
        #   <dt>flag</dt>
        #   <dd>A 16-bit integer indicating the state of the cell</dd>
        # </dl>
        class Key
          include ::Thrift::Struct, ::Thrift::Struct_Union
          ROW = 1
          COLUMN_FAMILY = 2
          COLUMN_QUALIFIER = 3
          TIMESTAMP = 4
          REVISION = 5
          FLAG = 6

          FIELDS = {
            ROW => {:type => ::Thrift::Types::STRING, :name => 'row'},
            COLUMN_FAMILY => {:type => ::Thrift::Types::STRING, :name => 'column_family'},
            COLUMN_QUALIFIER => {:type => ::Thrift::Types::STRING, :name => 'column_qualifier'},
            TIMESTAMP => {:type => ::Thrift::Types::I64, :name => 'timestamp', :optional => true},
            REVISION => {:type => ::Thrift::Types::I64, :name => 'revision', :optional => true},
            FLAG => {:type => ::Thrift::Types::I16, :name => 'flag', :default => 255}
          }

          def struct_fields; FIELDS; end

          def validate
          end

          ::Thrift::Struct.generate_accessors self
        end

        # Specifies options for a shared periodic mutator
        # 
        # <dl>
        #   <dt>appname</dt>
        #   <dd>String key used to share/retrieve mutator, eg: "my_ht_app"</dd>
        # 
        #   <dt>flush_interval</dt>
        #   <dd>Time interval between flushes</dd>
        # 
        #   <dt>flags</dt>
        #   <dd>Mutator flags</dt>
        # </dl>
        class MutateSpec
          include ::Thrift::Struct, ::Thrift::Struct_Union
          APPNAME = 1
          FLUSH_INTERVAL = 2
          FLAGS = 3

          FIELDS = {
            APPNAME => {:type => ::Thrift::Types::STRING, :name => 'appname', :default => %q""},
            FLUSH_INTERVAL => {:type => ::Thrift::Types::I32, :name => 'flush_interval', :default => 1000},
            FLAGS => {:type => ::Thrift::Types::I32, :name => 'flags', :default => 2}
          }

          def struct_fields; FIELDS; end

          def validate
            raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field appname is unset!') unless @appname
            raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field flush_interval is unset!') unless @flush_interval
            raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field flags is unset!') unless @flags
          end

          ::Thrift::Struct.generate_accessors self
        end

        # Defines a table cell
        # 
        # <dl>
        #   <dt>key</dt>
        #   <dd>Specifies the cell key</dd>
        # 
        #   <dt>value</dt>
        #   <dd>Value of a cell. Currently a sequence of uninterpreted bytes.</dd>
        # </dl>
        class Cell
          include ::Thrift::Struct, ::Thrift::Struct_Union
          KEY = 1
          VALUE = 2

          FIELDS = {
            KEY => {:type => ::Thrift::Types::STRUCT, :name => 'key', :class => Hypertable::ThriftGen::Key},
            VALUE => {:type => ::Thrift::Types::STRING, :name => 'value', :binary => true, :optional => true}
          }

          def struct_fields; FIELDS; end

          def validate
          end

          ::Thrift::Struct.generate_accessors self
        end

        # Defines a table split
        # 
        # <dl>
        #   <dt>start_row</dt>
        #   <dd>Starting row of the split.</dd>
        # 
        #   <dt>end_row</dt>
        #   <dd>Ending row of the split.</dd>
        # 
        #   <dt>location</dt>
        #   <dd>Location (proxy name) of the split.</dd>
        # 
        #   <dt>ip_address</dt>
        #   <dd>The IP address of the split.</dd>
        # </dl>
        class TableSplit
          include ::Thrift::Struct, ::Thrift::Struct_Union
          START_ROW = 1
          END_ROW = 2
          LOCATION = 3
          IP_ADDRESS = 4

          FIELDS = {
            START_ROW => {:type => ::Thrift::Types::STRING, :name => 'start_row', :optional => true},
            END_ROW => {:type => ::Thrift::Types::STRING, :name => 'end_row', :optional => true},
            LOCATION => {:type => ::Thrift::Types::STRING, :name => 'location', :optional => true},
            IP_ADDRESS => {:type => ::Thrift::Types::STRING, :name => 'ip_address', :optional => true}
          }

          def struct_fields; FIELDS; end

          def validate
          end

          ::Thrift::Struct.generate_accessors self
        end

        # Describes a ColumnFamily
        # <dl>
        #   <dt>name</dt>
        #   <dd>Name of the column family</dd>
        # 
        #   <dt>ag</dt>
        #   <dd>Name of the access group for this CF</dd>
        # 
        #   <dt>max_versions</dt>
        #   <dd>Max versions of the same cell to be stored</dd>
        # 
        #   <dt>ttl</dt>
        #   <dd>Time to live for cells in the CF (ie delete cells older than this time)</dd>
        # </dl>
        class ColumnFamily
          include ::Thrift::Struct, ::Thrift::Struct_Union
          NAME = 1
          AG = 2
          MAX_VERSIONS = 3
          TTL = 4

          FIELDS = {
            NAME => {:type => ::Thrift::Types::STRING, :name => 'name', :optional => true},
            AG => {:type => ::Thrift::Types::STRING, :name => 'ag', :optional => true},
            MAX_VERSIONS => {:type => ::Thrift::Types::I32, :name => 'max_versions', :optional => true},
            TTL => {:type => ::Thrift::Types::STRING, :name => 'ttl', :optional => true}
          }

          def struct_fields; FIELDS; end

          def validate
          end

          ::Thrift::Struct.generate_accessors self
        end

        # Describes an AccessGroup
        # <dl>
        #   <dt>name</dt>
        #   <dd>Name of the access group</dd>
        # 
        #   <dt>in_memory</dt>
        #   <dd>Is this access group in memory</dd>
        # 
        #   <dt>replication</dt>
        #   <dd>Replication factor for this AG</dd>
        # 
        #   <dt>blocksize</dt>
        #   <dd>Specifies blocksize for this AG</dd>
        # 
        #   <dt>compressor</dt>
        #   <dd>Specifies compressor for this AG</dd>
        # 
        #   <dt>bloom_filter</dt>
        #   <dd>Specifies bloom filter type</dd>
        # 
        #   <dt>columns</dt>
        #   <dd>Specifies list of column families in this AG</dd>
        # </dl>
        class AccessGroup
          include ::Thrift::Struct, ::Thrift::Struct_Union
          NAME = 1
          IN_MEMORY = 2
          REPLICATION = 3
          BLOCKSIZE = 4
          COMPRESSOR = 5
          BLOOM_FILTER = 6
          COLUMNS = 7

          FIELDS = {
            NAME => {:type => ::Thrift::Types::STRING, :name => 'name', :optional => true},
            IN_MEMORY => {:type => ::Thrift::Types::BOOL, :name => 'in_memory', :optional => true},
            REPLICATION => {:type => ::Thrift::Types::I16, :name => 'replication', :optional => true},
            BLOCKSIZE => {:type => ::Thrift::Types::I32, :name => 'blocksize', :optional => true},
            COMPRESSOR => {:type => ::Thrift::Types::STRING, :name => 'compressor', :optional => true},
            BLOOM_FILTER => {:type => ::Thrift::Types::STRING, :name => 'bloom_filter', :optional => true},
            COLUMNS => {:type => ::Thrift::Types::LIST, :name => 'columns', :element => {:type => ::Thrift::Types::STRUCT, :class => Hypertable::ThriftGen::ColumnFamily}, :optional => true}
          }

          def struct_fields; FIELDS; end

          def validate
          end

          ::Thrift::Struct.generate_accessors self
        end

        # Describes a schema
        # <dl>
        #   <dt>name</dt>
        #   <dd>Name of the access group</dd>
        # 
        #   <dt>in_memory</dt>
        #   <dd>Is this access group in memory</dd>
        # 
        #   <dt>replication</dt>
        #   <dd>Replication factor for this AG</dd>
        # 
        #   <dt>blocksize</dt>
        #   <dd>Specifies blocksize for this AG</dd>
        # 
        #   <dt>compressor</dt>
        #   <dd>Specifies compressor for this AG</dd>
        # 
        #   <dt>bloom_filter</dt>
        #   <dd>Specifies bloom filter type</dd>
        # 
        #   <dt>columns</dt>
        #   <dd>Specifies list of column families in this AG</dd>
        # </dl>
        class Schema
          include ::Thrift::Struct, ::Thrift::Struct_Union
          ACCESS_GROUPS = 1
          COLUMN_FAMILIES = 2

          FIELDS = {
            ACCESS_GROUPS => {:type => ::Thrift::Types::MAP, :name => 'access_groups', :key => {:type => ::Thrift::Types::STRING}, :value => {:type => ::Thrift::Types::STRUCT, :class => Hypertable::ThriftGen::AccessGroup}, :optional => true},
            COLUMN_FAMILIES => {:type => ::Thrift::Types::MAP, :name => 'column_families', :key => {:type => ::Thrift::Types::STRING}, :value => {:type => ::Thrift::Types::STRUCT, :class => Hypertable::ThriftGen::ColumnFamily}, :optional => true}
          }

          def struct_fields; FIELDS; end

          def validate
          end

          ::Thrift::Struct.generate_accessors self
        end

        # Exception for thrift clients.
        # 
        # <dl>
        #   <dt>code</dt><dd>Internal use (defined in src/cc/Common/Error.h)</dd>
        #   <dt>message</dt><dd>A message about the exception</dd>
        # </dl>
        # 
        # Note: some languages (like php) don't have adequate namespace, so Exception
        # would conflict with language builtins.
        class ClientException < ::Thrift::Exception
          include ::Thrift::Struct, ::Thrift::Struct_Union
          CODE = 1
          MESSAGE = 2

          FIELDS = {
            CODE => {:type => ::Thrift::Types::I32, :name => 'code'},
            MESSAGE => {:type => ::Thrift::Types::STRING, :name => 'message'}
          }

          def struct_fields; FIELDS; end

          def validate
          end

          ::Thrift::Struct.generate_accessors self
        end

      end
    end
