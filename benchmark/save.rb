require File.join(File.dirname(__FILE__), '../spec/spec_helper.rb')

# Results:
# Using with_mutator on each save:
# 1000 save requests in 17.621323 sec: 56.7494279515789 r/s
# 1000 save requests in 15.505761 sec: 64.4921587531241 r/s
# 1000 save requests in 16.581315 sec: 60.3088476396474 r/s

class Bench < ActiveRecord::HyperBase
  def self.create_table
    hql = "CREATE TABLE #{table_name} (
      'value' MAX_VERSIONS=1
    )"
    connection.execute(hql)
  end
end

Bench.drop_table if Bench.table_exists?
Bench.create_table

n = 1000

t1 = Time.now
for i in 1..n
  record = Bench.new({:ROW => i, :value => i})
  record.save!
end
ts1 = Time.now - t1

puts "#{n} save requests in #{ts1} sec: #{n/ts1} r/s"
