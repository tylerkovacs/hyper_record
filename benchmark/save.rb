require File.join(File.dirname(__FILE__), '../spec/spec_helper.rb')

# Results with flush_interval=10
# 1000 save              requests in 11.829525 sec: 84.5342479938966 r/s
# 1000 save_with_mutator requests in 1.781124 sec: 561.443223492581 r/s
#
# Results with flush_interval=100
# 1000 save              requests in 13.218057 sec: 75.6540844089264 r/s
# 1000 save_with_mutator requests in 0.990271 sec: 1009.82458337162 r/s

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
puts "#{n} save              requests in #{ts1} sec: #{n/ts1} r/s"

Bench.drop_table if Bench.table_exists?
Bench.create_table

flush_interval = 100
t2 = Time.now
m = Bench.open_mutator 
for i in 1..n
  record = Bench.new({:ROW => i, :value => i})
  record.save_with_mutator!(m)
  Bench.flush_mutator(m) if i % flush_interval == 0
end
Bench.close_mutator(m)
ts2 = Time.now - t2
puts "#{n} save_with_mutator requests in #{ts2} sec: #{n/ts2} r/s"
