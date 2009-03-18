# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{hyper_record}
  s.version = "0.2.2"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["tylerkovacs"]
  s.date = %q{2009-03-17}
  s.description = %q{See README}
  s.email = %q{tyler.kovacs@gmail.com}
  s.files = ["VERSION.yml", "lib/associations", "lib/associations/hyper_has_many_association_extension.rb", "lib/associations/hyper_has_and_belongs_to_many_association_extension.rb", "lib/active_record", "lib/active_record/connection_adapters", "lib/active_record/connection_adapters/hypertable_adapter.rb", "lib/active_record/connection_adapters/hyper_table_definition.rb", "lib/active_record/connection_adapters/qualified_column.rb", "lib/hypertable", "lib/hypertable/thrift_transport_monkey_patch.rb", "lib/hypertable/gen-rb", "lib/hypertable/gen-rb/ClientService.rb", "lib/hypertable/gen-rb/Hql_constants.rb", "lib/hypertable/gen-rb/Hql_types.rb", "lib/hypertable/gen-rb/HqlService.rb", "lib/hypertable/gen-rb/Client_constants.rb", "lib/hypertable/gen-rb/Client_types.rb", "lib/hypertable/thrift_client.rb", "lib/hyper_record.rb", "test/thrift_client_test.rb", "test/test_helper.rb", "spec/fixtures", "spec/fixtures/qualified_pages.yml", "spec/fixtures/pages.yml", "spec/spec_helper.rb", "spec/lib", "spec/lib/associations_spec.rb", "spec/lib/hyper_record_spec.rb", "spec/lib/hypertable_adapter_spec.rb"]
  s.has_rdoc = true
  s.homepage = %q{http://github.com/tylerkovacs/hyper_record}
  s.rdoc_options = ["--inline-source", "--charset=UTF-8"]
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.1}
  s.summary = %q{Fully integrates ActiveRecord with Hypertable.}

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 2

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
    else
    end
  else
  end
end
