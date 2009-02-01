require File.join(File.dirname(__FILE__), '../spec_helper.rb')

class Book < ActiveRecord::HyperBase
  has_many :chapters
  has_and_belongs_to_many :authors
  qualified_column :author_id, :qualifiers => ['.*']
  qualified_column :chapter_id, :qualifiers => ['.*']

  def self.create_table
    hql = "CREATE TABLE #{table_name} (
      'title',
      'author_id',
      'chapter_id'
    )"
    connection.execute(hql)
  end
end

class Chapter < ActiveRecord::HyperBase
  belongs_to :book

  def self.create_table
    hql = "CREATE TABLE #{table_name} (
      'title',
      'book_id' MAX_VERSIONS=1
    )"
    connection.execute(hql)
  end
end

class Author < ActiveRecord::HyperBase
  has_and_belongs_to_many :books
  qualified_column :book_id, :qualifiers => ['.*']

  def self.create_table
    hql = "CREATE TABLE #{table_name} (
      'name',
      'book_id'
    )"
    connection.execute(hql)
  end
end

module ActiveRecord
  module HyperRecord
    describe HyperBase, '.has_and_belongs_to_many' do
      before(:each) do
        Book.drop_table
        Author.drop_table
        Book.create_table
        Author.create_table

        @b = Book.new({:title => "Curious George and the Electric Fence"})
        @b.ROW = 'curious_george'
        @b.save!

        @a1 = Author.new({:name => 'Irvine Welsh', :ROW => 'irvine_welsh'})
        @a1.save!
        @a2 = Author.new({:name => 'Douglas Adams', :ROW => 'douglas_adams'})
        @a2.save!
      end

      it "should support addition of association elements using <<" do
        @b.authors.should be_empty
        @b.authors << @a1
        @b.authors.should == [@a1]
        @b.reload
        @b.authors.should == [@a1]
        @a1.books.should == [@b]
      end

      it "should allow multiple objects to be associated through HABTM" do
        @b.authors.should be_empty
        @b.authors << @a1
        @b.authors << @a2
        @b.authors.map{|x| x.ROW}.sort.should == [@a1, @a2].map{|x| x.ROW}.sort
        @b.reload
        @b.authors.map{|x| x.ROW}.sort.should == [@a1, @a2].map{|x| x.ROW}.sort
        @a1.books.map{|x| x.ROW}.sort.should == [@b].map{|x| x.ROW}.sort
        @a2.books.map{|x| x.ROW}.sort.should == [@b].map{|x| x.ROW}.sort
      end

      it "should allow removal of association elements using clear" do
        @b.authors.should be_empty
        @b.authors << @a1
        @b.authors.should == [@a1]
        @b.authors.clear
        @b.reload
        @b.authors.should be_empty
      end

      it "should allow an object to be created through the association" do
        a = @b.authors.create({:name => 'Harper Lee', :ROW => 'harper_lee'})
        a.new_record?.should be_false
        a.reload
        a.books.should == [@b]
      end

      it "should allow an object to be newed through the association" do
        @b.authors.should be_empty
        a = @b.authors.new({:name => 'Harper Lee', :ROW => 'harper_lee'})
        a.new_record?.should be_true
        a.save!
        a.reload
        a.books.should be_empty
      end

      it "should allow removal of association elements using delete" do
        @b.authors.should be_empty
        @b.authors << @a1
        @b.authors << @a2
        @b.authors.delete(@a2)
        @b.reload
        @b.authors.should == [@a1]
      end

      it "should clean up association cells when an object is destroyed" do
        @b.authors.should be_empty
        @b.authors << @a1
        @b.author_id.should == {@a1.ROW => 1}
        @a1.destroy
        @b.reload
        @b.author_id.should == {}
      end
    end

    describe HyperBase, '.belongs_to_and_has_many' do
      before(:each) do
        Book.drop_table
        Chapter.drop_table
        Book.create_table
        Chapter.create_table

        @b = Book.new({:title => "Curious George and the Electric Fence"})
        @b.ROW = 'curious_george'
        @b.save!

        @c1 = Chapter.new({:title => 'Ch 1', :ROW => 'c1'})
        @c1.save!
        @b.chapters << @c1
        @c2 = Chapter.new({:title => 'Ch 2', :ROW => 'c2'})
        @c2.save!
      end

      it "should allow belongs_to assocs between two hyperrecord objects" do
        @c1.book.should == @b
        @c2.book.should == nil
        @b.chapters.to_a.should == [@c1]
        @b.chapters << @c2
        @b.reload
        @b.chapters.to_a.should == [@c1, @c2]
        @c2.book_id.should == @b.ROW
      end

      it "should clear has_many associations when requested" do
        @b.chapters.to_a.should == [@c1]
        @b.chapters.clear
        @b.reload
        @b.chapters.to_a.should be_empty
        @b.chapter_id.should == {}
      end

      it "should allow new records through has_many but note that the association cells are not written, so this method is to be avoided" do
        @b.chapters.to_a.should == [@c1]
        c = @b.chapters.new({:ROW => 'c3', :title => 'Ch 3'})
        c.new_record?.should be_true
        c.save!
        @b.reload
        @b.chapters.length.should == 1
        @b.chapters.should == [@c1]
        @b.chapter_id.should == {@c1.ROW => "1"}
        @b.chapters << c
        @b.reload
        @b.chapters.should == [@c1, c]
        @b.chapter_id.should == {@c1.ROW => "1", c.ROW => "1"}
      end

      it "should allow create records through has_many" do
        @b.chapters.to_a.should == [@c1]
        c = @b.chapters.create({:ROW => 'c3', :title => 'Ch 3'})
        c.new_record?.should be_false
        @b.reload
        @b.chapters.length.should == 2
        @b.chapters.should == [@c1, c]
      end

      it "should allow new records using << has_many" do
        @b.chapters.to_a.should == [@c1]
        c = Chapter.new({:ROW => 'c3', :title => 'Ch 3'})
        c.new_record?.should be_true
        @b.chapters << c
        @b.reload
        @b.chapters.length.should == 2
        @b.chapters.should == [@c1, c]
        c.reload
        c.book.should == @b
      end

      it "should support remove of has_many records through delete" do
        @b.chapters.to_a.should == [@c1]
        c = @b.chapters.create({:ROW => 'c3', :title => 'Ch 3'})
        @b.reload
        @b.chapters.should == [@c1, c]
        @b.chapter_id.should == {@c1.ROW => "1", c.ROW => "1"}
        @b.chapters.delete(@c1)
        @b.reload
        @b.chapters.should == [c]
        @b.chapter_id.should == {c.ROW => "1"}
      end

      it "should update belongs_to id value on assignment" do
        @c2.book_id.should be_blank
        @c2.book = @b
        @c2.save!
        @c2.reload
        @c2.book_id.should == @b.id
      end

      it "should silently ignore eager loading of belongs_to associations" do
        c1 = Chapter.find(@c1.ROW, :include => [:book])
        # note: no exception, loaded? is marked as true and assoc still works
        c1.book.loaded?.should be_true
        c1.book.should == @b
      end

      it "should silently ignore eager loading of has_many associations" do
        b = Book.find(@b.ROW, :include => [:chapters])
        # note: no exception, loaded? is marked as false and assoc still works
        b.chapters.loaded?.should be_false
        b.chapters.to_a.should == [@c1]
      end
    end
  end
end
