require File.join(File.dirname(__FILE__), '../spec_helper.rb')

module ActiveRecord
  module HyperRecord
    describe HyperBase, '.describe_table' do
      fixtures :pages

      it "should return a string describing the table schema" do
        table_description = Page.connection.describe_table(Page.table_name)
        table_description.should_not be_empty
        table_description.should include("name")
        table_description.should include("url")
      end
    end

    describe HyperBase, '.table_exists?' do
      fixtures :pages

      it "should return true if the underlying table exists" do
        Page.table_exists?.should be_true
      end

      it "should return false if the underlying table does not exists" do
        Dummy.table_exists?.should be_false
      end
    end

    describe HyperBase, '.drop_table' do
      fixtures :pages

      it "should remove a table from hypertable" do
        Page.table_exists?.should be_true
        Page.drop_table
        Page.table_exists?.should be_false
      end
    end

    describe HyperBase, '.columns' do
      fixtures :pages

      it "should return an array of columns within the table" do
        table_columns = Page.columns
        table_columns.should_not be_empty
        # column list include the special ROW key.
        table_columns.map{|c| c.name}.should == ['ROW', 'name', 'url']
      end
    end

    describe HyperBase, '.column_families_without_row_key' do
      fixtures :pages

      it "should return an array of columns within the table but does not include the row key" do
        columns_without_row_key = Page.column_families_without_row_key
        columns_without_row_key.should_not be_empty
        # column list does not include the special ROW key.
        columns_without_row_key.map{|c| c.name}.should == ['name', 'url']
      end
    end

    describe HyperBase, '.qualified_column_names_without_row_key' do
      fixtures :pages

      it "should return an array of column names where column families are replaced by fully qualified columns" do
        cols = QualifiedPage.qualified_column_names_without_row_key
        cols.should_not be_empty
        cols.should == ['misc:name', 'misc:url', 'misc2:foo', 'misc2:bar']
      end
    end

    describe HyperBase, '.qualified_columns' do
      it "should include qualified columns in the regular column list" do
        columns = QualifiedPage.columns
        columns.should_not be_empty
        columns.map{|c| c.name}.should == ['ROW', 'misc', 'misc2']
      end
    end

    describe HyperBase, '.find_by_hql' do
      fixtures :pages

      it "should return the cells matching the hql specified" do
        pages = Page.find_by_hql("SELECT * FROM pages LIMIT=1")
        pages.length.should == 1
        page = pages.first
        page.class.should == Page
        page.name.should == "LOLcats and more"
        page.url.should == "http://www.icanhascheezburger.com"
      end

      it "should respond to the find_by_sql alias" do
        pages = Page.find_by_hql("SELECT * FROM pages LIMIT=1")
        pages.length.should == 1
        page = pages.first
        page.class.should == Page
        page.name.should == "LOLcats and more"
        page.url.should == "http://www.icanhascheezburger.com"
      end
    end

    describe HyperBase, '.find_initial' do
      fixtures :pages

      it "should return the first row in the table" do
        page = Page.find_initial({})
        page.class.should == Page
        page.name.should == "LOLcats and more"
        page.url.should == "http://www.icanhascheezburger.com"
      end
    end

    describe HyperBase, '.find_one' do
      fixtures :pages

      it "should return the requested row from the table" do
        page = Page.find_one('page_1', {})
        page.class.should == Page
        page.name.should == "LOLcats and more"
        page.url.should == "http://www.icanhascheezburger.com"
      end
    end

    describe HyperBase, '.find_some' do
      fixtures :pages

      it "should return the requested rows from the table" do
        row_keys = Page.find(:all).map{|p| p.ROW}
        record_count = row_keys.length
        record_count.should == 2
        pages = Page.find(row_keys)
        pages.length.should == record_count
      end
    end

    describe HyperBase, '.find' do
      fixtures :pages, :qualified_pages

      it "should return the declared list of qualified columns by default" do
        qp = QualifiedPage.new
        qp.new_record?.should be_true
        qp.misc['name'] = 'new page'
        qp.misc['url']= 'new.com'
        qp.ROW = 'new_qualified_page'
        qp.save.should be_true

        qp = QualifiedPage.find('new_qualified_page')
        qp.misc.keys.sort.should == ['name', 'url']
      end

      it "should support the limit option" do
        p = Page.new({:ROW => 'row key', :name => 'new entry'})
        p.save.should be_true
        Page.find(:all).length.should == 3
        pages = Page.find(:all, :limit => 2)
        pages.length.should == 2
      end

      it "should not support finder conditions not in Hash format" do
        lambda {Page.find(:all, :conditions => "value = 1")}.should raise_error
      end

      it "should not support finder conditions in Hash format" do
        # NOTE: will be supported in the future when Hypertable supports
        # efficient lookup on arbitrary columns
        lambda {
          pages = Page.find(:all, :conditions => {:name => 'ESPN'})
          pages.length.should == 1
          p = pages.first
          p.name.should == 'ESPN'
          p.ROW.should == 'page_2'
        }.should raise_error
      end

      it "should not support finder conditions in Hash format when the value is an array" do
        # NOTE: will be supported in the future when Hypertable supports
        # efficient lookup on arbitrary columns
        lambda {
          all_pages = Page.find(:all)
          all_pages.length.should == 2
          name_values = all_pages.map{|p| p.name}
          pages = Page.find(:all, :conditions => {:name => name_values})
          pages.length.should == 2
        }.should raise_error
      end

      it "should return a specific list of qualifiers when requested explicitly in finder options" do
        qp = QualifiedPage.new
        qp.new_record?.should be_true
        qp.misc['name'] = 'new page'
        qp.misc['url']= 'new.com'
        qp.ROW = 'new_qualified_page'
        qp.save.should be_true

        qp = QualifiedPage.find('new_qualified_page', :select => 'misc:url')
        # NOTE: will be supported in the future when Hypertable supports
        # efficient lookup on arbitrary columns
        # qp.misc.keys.sort.should == ['url']
        # For now, it returns all columns
        qp.misc.keys.sort.should == ['name', 'url']
      end

      describe ':select option' do
        before(:each) do
          @qpweq = QualifiedPageWithoutExplicitQualifiers.new
          @qpweq.new_record?.should be_true
          @qpweq.misc['name'] = 'new page'
          @qpweq.misc['url'] = 'new.com'
          @qpweq.ROW = 'new_qualified_page'
          @qpweq.save.should be_true
        end

        it "should return an empty hash for all qualified columns even if none are explicitly listed in qualifiers" do
          @qpweq2 = QualifiedPageWithoutExplicitQualifiers.find(@qpweq.ROW)
          @qpweq2.misc.should == {}
          @qpweq2.misc2.should == ""
        end

        it "should return correct values for qualified columns named in select list using comma separated string" do
          qpweq2 = QualifiedPageWithoutExplicitQualifiers.find(@qpweq.ROW, :select => "misc,misc2")
          qpweq2.misc.should == {"name"=>"new page", "url"=>"new.com"}
          qpweq2.misc2.should == ""
        end

        it "should return correct values for qualified columns named in select list using array" do
          qpweq2 = QualifiedPageWithoutExplicitQualifiers.find(@qpweq.ROW, :select => ["misc", "misc2"])
          qpweq2.misc.should == {"name"=>"new page", "url"=>"new.com"}
          qpweq2.misc2.should == ""
        end
      end

      it "should instantiate the object with empty hashes for qualified columns when no explicit select list is supplied" do
        qp = QualifiedPage.new
        qp.new_record?.should be_true
        qp.ROW = 'new_qualified_page'
        qp.misc2['name'] = 'test'
        qp.save.should be_true

        qp = QualifiedPage.find(:first)
        qp.misc.should == {}
      end

      it "should allow user to specify ROW key as part of initialize attributes" do
        p = Page.new({:ROW => 'row key'})
        p.ROW.should == 'row key'
      end

      it "should not have any residual state between calls to new" do
        qp = QualifiedPage.new
        qp.new_record?.should be_true
        qp.misc['name'] = 'new page'
        qp.misc['url']= 'new.com'
        qp.ROW = 'new_qualified_page'
        qp.save.should be_true

        qp2 = QualifiedPage.new
        qp2.misc.should == {}
        qp2.misc2.should == {}
        qp.misc.object_id.should_not == qp2.misc.object_id
      end
    end

    describe HyperBase, '.table_exists?' do
      fixtures :pages

      it "should return true for a table that does exist" do
        Page.table_exists?.should be_true
      end

      it "should return false for a table that does exist" do
        Dummy.table_exists?.should be_false
      end
    end

    describe HyperBase, '.primary_key' do
      it "should always return the special ROW key" do
        Dummy.primary_key.should == 'ROW'
      end
    end

    describe HyperBase, '.new' do
      fixtures :pages, :qualified_pages

      it "should an object of correct class" do
        p = Page.new
        p.new_record?.should be_true
        p.class.should == Page
        p.class.should < ActiveRecord::HyperBase
        p.attributes.keys.sort.should == ['name', 'url']
      end

      it "should not allow an object to be saved without a row key" do
        page_count = Page.find(:all).length
        p = Page.new
        p.new_record?.should be_true
        p.name = "new page"
        p.url = "new.com"
        p.valid?.should be_false
        p.save.should be_false
        p.new_record?.should be_true
        p.ROW = "new_page"
        p.valid?.should be_true
        p.save.should be_true
        p.new_record?.should be_false
        Page.find(:all).length.should == page_count + 1
      end

      it "should save a table with qualified columns correctly" do
        qp = QualifiedPage.new
        qp.new_record?.should be_true
        qp.misc['name'] = 'new page'
        qp.misc['url']= 'new.com'
        qp.ROW = 'new_qualified_page'
        qp.save.should be_true
        qp.new_record?.should be_false
        qp.reload.should == qp
        qp.misc['name'].should == 'new page'
        qp.misc['url'].should == 'new.com'
        qp.misc.keys.sort.should == ['name', 'url']
      end
    end

    describe HyperBase, '.reload' do
      fixtures :pages

      it "should reload an object and revert any changed state" do
        p = Page.find(:first)
        p.class.should == Page
        original_url = p.url.clone
        p.url = "new url"
        p.reload.should == p
        p.url.should == original_url
      end
    end

    describe HyperBase, '.save' do
      fixtures :pages, :qualified_pages

      it "should update an object in hypertable" do
        p = Page.find(:first)
        p.class.should == Page
        original_url = p.url.clone
        p.url = "new url"
        p.save.should be_true
        p.url.should == "new url"
        p.reload.should == p
        p.url.should == "new url"
      end

      it "should allow undeclared qualified columns to be saved, provided that the column family is declared" do
        qp = QualifiedPage.new
        qp.new_record?.should be_true
        qp.misc['name'] = 'new page'
        qp.misc['url'] = 'new.com'
        qp.misc['new_column'] = 'value'
        qp.ROW = 'new_qualified_page'
        qp.save.should be_true
        qp.new_record?.should be_false
        qp.reload.should == qp
        qp.misc['name'].should == 'new page'
        qp.misc['url'].should == 'new.com'
        qp.misc['new_column'].should == 'value'
      end
    end

    describe HyperBase, '.save_with_mutator' do
      fixtures :pages

      it "should successfully save an object with mutator" do
        m = Page.open_mutator
        p1 = Page.new({:ROW => 'created_with_mutator_1', :url => 'url_1'})
        p1.save_with_mutator!(m)

        p2 = Page.new({:ROW => 'created_with_mutator_2', :url => 'url_2'})
        p2.save_with_mutator!(m)

        Page.close_mutator(m)

        new_page_1 = Page.find('created_with_mutator_1')
        new_page_1.url.should == 'url_1'

        new_page_2 = Page.find('created_with_mutator_2')
        new_page_2.url.should == 'url_2'
      end

      it "should not flush the mutator and not create objects when flush is not requested on close mutator" do
        m = Page.open_mutator
        p1 = Page.new({:ROW => 'created_with_mutator_1', :url => 'url_1'})
        p1.save_with_mutator!(m)
        Page.close_mutator(m, false)

        lambda {
          Page.find('created_with_mutator_1')
        }.should raise_error(::ActiveRecord::RecordNotFound)
      end

      it "should support explicit flushing of the mutator" do
        m = Page.open_mutator
        p1 = Page.new({:ROW => 'created_with_mutator_1', :url => 'url_1'})
        p1.save_with_mutator!(m)
        Page.flush_mutator(m)
        Page.close_mutator(m, false)

        new_page_1 = Page.find('created_with_mutator_1')
        new_page_1.url.should == 'url_1'
      end
    end

    describe HyperBase, '.update' do
      fixtures :pages

      it "should update an object in hypertable" do
        p = Page.find(:first)
        p.class.should == Page
        original_url = p.url.clone
        p.url = "new url"
        p.update.should be_true
        p.url.should == "new url"
        p.reload.should == p
        p.url.should == "new url"
      end
    end

    describe HyperBase, '.destroy' do
      fixtures :pages

      it "should remove an object from hypertable" do
        p = Page.find(:first)
        p.reload.should == p
        p.destroy
        lambda {p.reload}.should raise_error(::ActiveRecord::RecordNotFound)
      end

      it "should remove an object from hypertable based on the id" do
        p = Page.find(:first)
        p.reload.should == p
        Page.destroy(p.ROW)
        lambda {p.reload}.should raise_error(::ActiveRecord::RecordNotFound)
      end

      it "should remove multiple objects from hypertable based on ids" do
        pages = Page.find(:all)
        pages.length.should == 2
        Page.destroy(pages.map{|p| p.ROW})
        pages = Page.find(:all)
        pages.length.should == 0
      end
    end

    describe HyperBase, '.delete' do
      fixtures :pages

      it "should remove an object from hypertable based on the id" do
        p = Page.find(:first)
        p.reload.should == p
        Page.delete(p.ROW)
        lambda {p.reload}.should raise_error(::ActiveRecord::RecordNotFound)
      end

      it "should remove multiple objects from hypertable based on ids" do
        pages = Page.find(:all)
        pages.length.should == 2
        Page.delete(pages.map{|p| p.ROW})
        pages = Page.find(:all)
        pages.length.should == 0
      end
    end

    describe HyperBase, '.exists' do
      fixtures :pages

      it "should confirm that a record exists" do
        p = Page.find(:first)
        Page.exists?(p.ROW).should be_true
      end

      it "should refute that a record does not exists" do
        Page.exists?('foofooofoofoofoofoomonkey').should be_false
      end

      it "should not support arguments that are not numbers, strings or hashes" do
        lambda {Page.exists?([1])}.should raise_error
      end

      it "should not allow a Hash argument for conditions" do
        lambda {
          Page.exists?(:name => 'ESPN').should be_true
        }.should raise_error


        lambda {
          Page.find(:first, :conditions => {:name => 'ESPN'}).should_not be_nil
        }.should raise_error

        lambda {
          Page.exists?(:name => 'foofoofoofoofoo').should be_false
        }.should raise_error

        lambda {
          Page.find(:first, :conditions => {:name => 'foofoofoofoofoo'}).should be_nil
        }.should raise_error
      end
    end

    describe HyperBase, '.increment' do
      fixtures :pages

      it "should increment an integer value" do
        p = Page.find(:first)
        p.name = 7
        p.save
        p.reload
        p.increment('name')
        p.name.should == 8
        p.save
        p.reload
        p.name.should == "8"
        p.increment('name', 2)
        p.save
        p.reload
        p.name.should == "10"
      end
    end

    describe HyperBase, '.increment!' do
      fixtures :pages

      it "should increment an integer value and save" do
        p = Page.find(:first)
        p.name = 7
        p.increment!('name')
        p.name.should == 8
        p.reload
        p.name.should == "8"
        p.increment!('name', 2)
        p.reload
        p.name.should == "10"
      end
    end

    describe HyperBase, '.decrement' do
      fixtures :pages

      it "should decrement an integer value" do
        p = Page.find(:first)
        p.name = 7
        p.save
        p.reload
        p.decrement('name')
        p.name.should == 6
        p.save
        p.reload
        p.name.should == "6"
        p.decrement('name', 2)
        p.save
        p.reload
        p.name.should == "4"
      end
    end

    describe HyperBase, '.decrement!' do
      fixtures :pages

      it "should decrement an integer value and save" do
        p = Page.find(:first)
        p.name = 7
        p.decrement!('name')
        p.name.should == 6
        p.reload
        p.name.should == "6"
        p.decrement!('name', 2)
        p.reload
        p.name.should == "4"
      end
    end

    describe HyperBase, '.update_attribute' do
      fixtures :pages

      it "should allow a single attribute to be updated" do
        p = Page.find(:first)
        p.update_attribute(:name, 'new name value')
        p.name.should == 'new name value'
        p.reload
        p.name.should == 'new name value'
      end

      it "should save changes to more than the named column because that's the way activerecord works" do
        p = Page.find(:first)
        p.name = "name"
        p.url = "url"
        p.save!
        p.url = "new url"
        p.update_attribute(:name, 'new name value')
        p.name.should == 'new name value'
        p.url.should == 'new url'
        p.reload
        p.name.should == 'new name value'
        p.url.should == 'new url'
      end
    end

    describe HyperBase, '.update_attributes' do
      fixtures :pages

      it "should allow multiple attributes to be updated" do
        p = Page.find(:first)
        p.update_attributes({:name => 'new name value', :url => 'http://new/'})
        p.name.should == 'new name value'
        p.url.should == 'http://new/'
        p.reload
        p.name.should == 'new name value'
        p.url.should == 'http://new/'
      end
    end

    describe HyperBase, '.attributes' do
      fixtures :pages

      describe '.attributes_with_quotes' do
        it "should return attributes in expected format" do
          p = Page.find(:first)
          attrs = p.attributes_with_quotes
          attrs.keys.sort.should == ["ROW", "name", "url"]
          attrs['ROW'].should == 'page_1'
          attrs['name'].should == 'LOLcats and more'
          attrs['url'].should == 'http://www.icanhascheezburger.com'
        end
      end

      describe '.attributes_from_column_definition' do
        fixtures :pages, :qualified_pages

        it "should return attributes in expected format for scalar columns" do
          p = Page.find(:first)
          attrs = p.send(:attributes_from_column_definition)
          attrs.should == {"name"=>"", "url"=>""}
        end

        it "should return attributes in expected format for qualified columns" do
          qp = QualifiedPage.new
          qp.new_record?.should be_true
          qp.misc['name'] = 'new page'
          qp.misc['url']= 'new.com'
          qp.ROW = 'new_qualified_page'
          qp.save.should be_true
          qp.reload
          attrs = qp.send(:attributes_from_column_definition)
          attrs.should == {"misc"=>{}, "misc2"=>{}}
        end
      end

      describe '.attributes_from_column_definition' do
        fixtures :pages, :qualified_pages

        it "should accept hash assignment to qualified columns" do
          qp = QualifiedPage.new
          qp.ROW = 'new_page'
          qp.new_record?.should be_true
          value = {'name' => 'new page', 'url' => 'new.com'}
          qp.misc = value
          qp.misc.should == value
          qp.save.should be_true
          qp.reload
          qp.misc.should == value
          qp.misc['another_key'] = "1"
          qp.misc.should == value.merge({'another_key' => "1"})
          qp.save.should be_true
          qp.reload
          qp.misc.should == value.merge({'another_key' => "1"})
        end
      end
    end
  end
end
