require File.join(File.dirname(__FILE__), '../spec_helper.rb')

module ActiveRecord
  module ConnectionAdapters
    describe HypertableAdapter do
      before do
        @h = HypertableAdapter.new(nil, nil, {})
      end

      describe HypertableAdapter, '.describe_table' do
        before do
          @describe_table_text = '<Schema generation="1">\n <AccessGroup name="default">\n <ColumnFamily id="1">\n <Name>message</Name> </ColumnFamily>\n <ColumnFamily id="2">\n <Name>date-time</Name>\n </ColumnFamily>\n </AccessGroup>\n </Schema>\n'
        end

        it "should return a string describing a table" do
          @h.should_receive(:describe_table).with('name').and_return(@describe_table_text)
          @h.describe_table('name').should == @describe_table_text
        end
      end

      describe HypertableAdapter, '.column' do
        before do
          @describe_table_text = '<Schema generation="1">\n <AccessGroup name="default">\n <ColumnFamily id="1">\n <Name>message</Name> </ColumnFamily>\n <ColumnFamily id="2">\n <Name>date-time</Name>\n </ColumnFamily>\n </AccessGroup>\n </Schema>\n'
        end

        it "should return an array of columns representing the table schema" do
          @h.stub!(:describe_table).with('name').and_return(@describe_table_text)
          columns = @h.columns('name')
          columns.should be_is_a(Array)
          columns.should have_exactly(3).columns
          # The first column within a Hypertable is always the row key.
          columns[0].name.should == "ROW"
          columns[1].name.should == "message"
          # notice that the original column name "date-time" is converted
          # to a Ruby-friendly column name "date_time"
          columns[2].name.should == "date_time"
        end

        it "should set up the name mappings between ruby and hypertable" do
          @h.stub!(:describe_table).with('name').and_return(@describe_table_text)
          columns = @h.columns('name')
          @h.hypertable_column_name('date_time', 'name').should == 'date-time'
        end
      end

      describe HypertableAdapter, '.quote_column_name' do
        it "should surround column name in single quotes" do
          @h.quote_column_name("date_time").should == "'date_time'"
        end
      end

      describe HypertableAdapter, '.rubify_column_name' do
        it "should change dashes to underscores in column names" do
          @h.rubify_column_name("date-time").should == "date_time"
        end
      end

      describe HypertableAdapter, '.tables' do
        before do
          @tables = ["table1", "table2"]
        end

        it "should return an array of table names" do
          @h.should_receive(:tables).and_return(@tables)
          @h.tables.should == @tables
        end
      end

      describe HypertableAdapter, '.quote' do
        it "should return empty string for nil values" do
          @h.quote(nil).should == ''
        end
      end

      describe HypertableAdapter, '.quote' do
        it "should return a quoted string for all non-nil values" do
          @h.quote(1).should == "1"
          @h.quote('happy').should == "happy"
        end
      end

      describe HypertableAdapter, '.is_qualified_column_name?' do
        it "should return false for regular columns" do
          status, family, qualifier = @h.is_qualified_column_name?("col1")
          status.should be_false
          family.should be_nil
          qualifier.should be_nil
        end

        it "should return true for qualified columns" do
          status, family, qualifier = @h.is_qualified_column_name?("col1:red")
          status.should be_true
          family.should == 'col1'
          qualifier.should == 'red'
        end
      end

      describe HypertableAdapter, '.convert_select_columns_to_array_of_columns(' do
        it "should accept an array as input" do
          @h.convert_select_columns_to_array_of_columns(["one", "two", "three"]).should == ["one", "two", "three"]
        end

        it "should accept a string as input and split the results on commas" do
          @h.convert_select_columns_to_array_of_columns("one,two,three").should == ["one", "two", "three"]
        end

        it "should strip whitespace from column names" do
          @h.convert_select_columns_to_array_of_columns(" one,two , three ").should == ["one", "two", "three"]
        end

        it "should return [] for a request on * columns" do
          @h.convert_select_columns_to_array_of_columns("*").should == []
        end
      end

      describe HypertableAdapter, '.create_table_sql' do
        it "should assemble correct hql for creating a table" do
          hql = @h.create_table_hql :new_table do |t|
            t.column :name, :string
            t.column :value, :string, :max_versions => 1
          end

          hql.should == "CREATE TABLE 'new_table' ( 'name' , 'value' MAX_VERSIONS=1 )"
        end
      end
    end
  end
end
