ENV["RAILS_ENV"] = "test"
require File.expand_path(File.join(File.dirname(__FILE__), "../../../../config/environment"))
require 'spec'
require 'spec/autorun' # required rspec 1.2.8?
require 'spec/rails'

ActiveRecord::Base.configurations['hypertable'] = {
  'adapter' => 'hypertable',
  'host' => 'localhost',
  'port' => '38080'
}
ActiveRecord::Base.establish_connection(:hypertable)

class Dummy < ActiveRecord::HyperBase
end

class Page < ActiveRecord::HyperBase
  self.establish_connection(:hypertable)

  # mutator_options :persistent => true, :flush_interval => 100

  def self.create_table
    hql = "CREATE TABLE #{table_name} (
      'name',
      'url'
    )"
    connection.execute(hql)
  end
end

class QualifiedPage < ActiveRecord::HyperBase
  self.establish_connection(:hypertable)
  qualified_column :misc, :qualifiers => [:name, :url]
  qualified_column :misc2, :qualifiers => [:foo, :bar]

  def self.create_table
    hql = "CREATE TABLE #{table_name} (
      'misc',
      'misc2'
    )"
    connection.execute(hql)
  end
end

class QualifiedPageWithoutExplicitQualifiers < ActiveRecord::HyperBase
  QualifiedPageWithoutExplicitQualifiers.set_table_name "qualified_pages"
  self.establish_connection(:hypertable)
  qualified_column :misc

  def self.create_table
    QualifiedPage.create_table
  end
end

Spec::Runner.configure do |config|
  # If you're not using ActiveRecord you should remove these
  # lines, delete config/database.yml and disable :active_record
  # in your config/boot.rb
  config.use_transactional_fixtures = false
  config.use_instantiated_fixtures  = false
  config.fixture_path = File.expand_path(File.join(File.dirname(__FILE__), 'fixtures'))

  # == Fixtures
  #
  # You can declare fixtures for each example_group like this:
  #   describe "...." do
  #     fixtures :table_a, :table_b
  #
  # Alternatively, if you prefer to declare them only once, you can
  # do so right here. Just uncomment the next line and replace the fixture
  # names with your fixtures.
  #
  config.global_fixtures = []

  #
  # If you declare global fixtures, be aware that they will be declared
  # for all of your examples, even those that don't use them.
  #
  # == Mock Framework
  #
  # RSpec uses it's own mocking framework by default. If you prefer to
  # use mocha, flexmock or RR, uncomment the appropriate line:
  #
  # config.mock_with :mocha
  # config.mock_with :flexmock
  # config.mock_with :rr
end

class Fixtures
  def self.create_fixtures(fixtures_directory, table_names, class_names = {})
    Page.drop_table
    Page.create_table
    QualifiedPage.drop_table
    QualifiedPage.create_table

    table_names = [table_names].flatten.map { |n| n.to_s }
    connection  = block_given? ? yield : ActiveRecord::Base.connection

    table_names_to_fetch = table_names.reject { |table_name| fixture_is_cached?(connection, table_name) }

    unless table_names_to_fetch.empty?
      ActiveRecord::Base.silence do
        connection.disable_referential_integrity do
          fixtures_map = {}

          fixtures = table_names_to_fetch.map do |table_name|
            fixtures_map[table_name] = Fixtures.new(connection, File.split(table_name.to_s).last, class_names[table_name.to_sym], File.join(fixtures_directory, table_name.to_s))
          end

          all_loaded_fixtures.update(fixtures_map)

          connection.transaction(:requires_new => true) do
            #fixtures.reverse.each {|fixture| fixture.delete_existing_fixtures}
            fixtures.each {|fixture| fixture.insert_fixtures}

            # Cap primary key sequences to max(pk).
            if connection.respond_to?(:reset_pk_sequence!)
              table_names.each do |table_name|
                connection.reset_pk_sequence!(table_name)
              end
            end
          end

          cache_fixtures(connection, fixtures_map)
        end
      end
    end
    cached_fixtures(connection, table_names)
  end
end
