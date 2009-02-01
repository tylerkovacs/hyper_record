module ActiveRecord
  module Associations
    module HyperHasManyAssociationExtension
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

      def insert_record_with_hypertable(record)
        if @reflection.klass <= ActiveRecord::HyperBase
          raise "missing ROW key on record" if record.ROW.blank?
          @owner.send(@reflection.association_foreign_key)[record.ROW] = 1
          @owner.write_cells([ [@owner.ROW, record.connection.qualified_column_name(@reflection.association_foreign_key, record.ROW), "1"] ])
          record.send("#{@reflection.primary_key_name}=", @owner.ROW)
          record.save
          self.reset
        else
          insert_record_without_hypertable(record)
        end
      end

      def delete_records_with_hypertable(records)
        if @reflection.klass <= ActiveRecord::HyperBase
          cells_to_delete = []
          records.each {|r|
            # remove association cells from in memory object
            r.send("#{@reflection.primary_key_name}=", nil)
            r.save

            # make list of cells that need to be removed from hypertable
            cells_to_delete << [@owner.ROW, @owner.connection.qualified_column_name(@reflection.association_foreign_key, r.ROW)]
          }

          @owner.delete_cells(cells_to_delete, @owner.class.table_name)
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
              insert_record_without_hypertable(record)
            }
          end
        end
    end

    class HasManyAssociation
      include HyperHasManyAssociationExtension
    end
  end
end
