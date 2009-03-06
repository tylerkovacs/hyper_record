module ActiveRecord
  module ConnectionAdapters
    # Like a regular database, each table in Hypertable has a fixed list
    # of columns.  However, Hypertable allows flexible schemas through the
    # use of column qualifiers.  Suppose a table is defined to have a single 
    # column called misc.
    #
    # CREATE TABLE pages (
    #   'misc'
    # )
    #
    # In Hypertable, each traditional database column is referred to as
    # a column family.  Each column family can have a theoretically infinite
    # number of qualified instances.  An instance of a qualified column
    # is referred to using the column_family:qualifer notation.  e.g.,
    #
    # misc:red
    # misc:green
    # misc:blue
    #
    # These qualified column instances do not need to be declared as part 
    # of the table schema.  The table schema itself does not provide
    # an indication of whether a column family has been used with qualifiers.
    # As a results, we must explicitly declare intent to use a column family
    # in a qualified manner in our class definition.  The resulting AR 
    # object models the column family as a Hash.
    #
    # class Page < ActiveRecord::HyperBase
    #   qualified_column :misc
    # end
    #
    # p = Page.new
    # p.ROW = 'page_1'
    # p.misc['url'] = 'http://www.zvents.com/'
    # p.misc['hits'] = 127
    # p.save

    class QualifiedColumn < Column
      attr_accessor :qualifiers

      def initialize(name, default, sql_type = nil, null = true)
        @qualifiers ||= []
        super
      end

      def klass
        Hash
      end

      def default
        # Unlike regular AR objects, the default value for a column must
        # be cloned.  This is to avoid copy-by-reference issues with {}
        # objects.  Without clone, all instances of the class will share
        # a reference to the same object.
        @default.clone
      end
    end
  end
end
