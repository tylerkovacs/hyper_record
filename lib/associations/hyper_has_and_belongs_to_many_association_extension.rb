# Since Hypertable does not support join within queries, the association
# information is written to each table using qualified columns instead of
# using a separate JOIN table (as is the case for has_and_belongs_to_many
# associations in a regular RDBMS).
#
# For instance, assume that you have two models (Book and Author) in a 
# has_and_belongs_to_many association. The HABTM association in a traditional
# RDBMS requires a join table (authors_books) to record the associations
# between objects. In Hypertable, instead of using a separate join table, 
# each table has a column dedicated to recording the associations. 
# Specifically, the books table has an author_id column and the authors 
# table has a book_id column. 
#
# Column qualifiers are used so we can record as many associated objects are 
# necessary. If an Author with the row key charles_dickens is added to a 
# Book with the row key tale_of_two_cities, then a cell called 
# author_id:charles_dickens is added to the Book object and a cell 
# called book_id:tale_of_two_cities is added to the Author object. The 
# value of the cells is inconsequential - their presence alone indicates 
# an association between the objects. 
#
# When an object in an HABTM association is destroyed, the corresponding 
# entries in the associated table are removed (warning: don't use delete 
# because it will leave behind stale association information)

module ActiveRecord
  module Associations
    module HyperHasAndBelongsToManyAssociationExtension
      def self.included(base)
        base.class_eval do
          alias_method :find_without_hypertable, :find
          alias_method :find, :find_with_hypertable

          alias_method :delete_records_without_hypertable, :delete_records
          alias_method :delete_records, :delete_records_with_hypertable

          alias_method :insert_record_without_hypertable, :insert_record
          alias_method :insert_record, :insert_record_with_hypertable

          alias_method :create_record_without_hypertable, :create_record
          alias_method :create_record, :create_record_with_hypertable
        end
      end

      def find_with_hypertable(*args)
        if @reflection.klass <= ActiveRecord::HyperBase
          associated_object_ids = @owner.send(@reflection.association_foreign_key).keys
          @reflection.klass.find(associated_object_ids)
        else
          find_without_hypertable(*args)
        end
      end

      # Record the association in the assocation columns.
      def insert_record_with_hypertable(record, force=true)
        if @reflection.klass <= ActiveRecord::HyperBase
          @owner.send(@reflection.association_foreign_key)[record.ROW] = 1
          @owner.write_cells([@owner.connection.cell_native_array(@owner.ROW, @reflection.association_foreign_key, record.ROW, "1")])
          record.send(@reflection.primary_key_name)[@owner.ROW] = 1
          record.write_cells([@owner.connection.cell_native_array(record.ROW, @reflection.primary_key_name, @owner.ROW, "1")])
        else
          insert_record_without_hypertable(record, force)
        end
      end

      # Remove the association from the assocation columns.
      def delete_records_with_hypertable(records)
        if @reflection.klass <= ActiveRecord::HyperBase
          cells_to_delete_by_table = Hash.new{|h,k| h[k] = []}

          records.each {|r|
            # remove association cells from in memory object
            @owner.send(@reflection.association_foreign_key).delete(r.ROW)
            r.send(@reflection.primary_key_name).delete(@owner.ROW)

            # make list of cells that need to be removed from hypertable
            cells_to_delete_by_table[@owner.class.table_name] << @owner.connection.cell_native_array(@owner.ROW, @reflection.association_foreign_key, r.ROW)
            cells_to_delete_by_table[r.class.table_name] << @owner.connection.cell_native_array(r.ROW, @reflection.primary_key_name, @owner.ROW)
          }

          for table in cells_to_delete_by_table.keys
            @owner.delete_cells(cells_to_delete_by_table[table], table)
          end
        else
          delete_records_without_hypertable(records)
        end
      end

      private
        def create_record_with_hypertable(attributes)
          if @reflection.klass <= ActiveRecord::HyperBase
            r = @reflection.klass.create(attributes)
            insert_record_with_hypertable(r)
            r
          else
            create_record_without_hypertable(attributes) {|record|
              insert_record_without_hypertable(record, true)
            }
          end
        end
    end

    class HasAndBelongsToManyAssociation
      include HyperHasAndBelongsToManyAssociationExtension
    end
  end
end
