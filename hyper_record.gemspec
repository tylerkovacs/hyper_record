Gem::Specification.new do |s|
  s.name = %q{hyper_record}
  s.version = "0.2.1"
  s.date = %q{2009-03-06}
  s.summary = %q{Fully integrates ActiveRecord with Hypertable.}
  s.email = %q{tyler.kovacs@gmail.com}
  s.homepage = %q{http://github.com/tylerkovacs/hyper_record}
  s.description = %q{See README}
  s.has_rdoc = true
  s.authors = ["tylerkovacs"]
  s.files = ["VERSION.yml", "lib/hyper_record.rb", "lib/hypertable", "lib/hypertable/gen-rb", "lib/hypertable/gen-rb/Client_constants.rb", "lib/hypertable/gen-rb/ClientService.rb", "lib/hypertable/gen-rb/Hql_types.rb", "lib/hypertable/gen-rb/Client_types.rb", "lib/hypertable/gen-rb/Hql_constants.rb", "lib/hypertable/gen-rb/HqlService.rb", "lib/hypertable/thrift_client.rb", "lib/active_record", "lib/active_record/connection_adapters", "lib/active_record/connection_adapters/qualified_column.rb", "lib/active_record/connection_adapters/hyper_table_definition.rb", "lib/active_record/connection_adapters/hypertable_adapter.rb", "lib/associations", "lib/associations/hyper_has_and_belongs_to_many_association_extension.rb", "lib/associations/hyper_has_many_association_extension.rb", "spec/lib", "spec/lib/associations_spec.rb", "spec/lib/hyper_record_spec.rb", "spec/fixtures", "spec/fixtures/qualified_pages.yml", "spec/fixtures/pages.yml", "spec/spec_helper.rb", "README", "LICENSE"]
  s.rdoc_options = ["--inline-source", "--charset=UTF-8"]
  s.extra_rdoc_files = ["README", "LICENSE"]
end
