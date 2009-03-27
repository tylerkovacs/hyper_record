require File.join(File.dirname(__FILE__), '../spec/spec_helper.rb')

# Results with flush_interval=10
# 1000 save              requests in 14.879574 sec: 67.2062251244559 r/s
#      hypertable write latency was 13.632525
# 1000 save_with_mutator requests in 1.624659 sec: 615.513778583691 r/s
#      hypertable write latency was 0.557777
#
# Results with flush_interval=100
# 1000 save              requests in 13.127422 sec: 76.1764191019379 r/s
#      hypertable write latency was 11.971095
# 1000 save_with_mutator requests in 0.886474 sec: 1128.06466969138 r/s
#      hypertable write latency was 0.454088000000001

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

read_latency, write_latency, cells_read = ActiveRecord::ConnectionAdapters::HypertableAdapter.get_timing
ActiveRecord::ConnectionAdapters::HypertableAdapter.reset_timing
puts "#{n} save              requests in #{ts1} sec: #{n/ts1} r/s"
puts "     hypertable write latency was #{write_latency}"

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

read_latency, write_latency, cells_read = ActiveRecord::ConnectionAdapters::HypertableAdapter.get_timing
ActiveRecord::ConnectionAdapters::HypertableAdapter.reset_timing
puts "#{n} save_with_mutator requests in #{ts2} sec: #{n/ts2} r/s"
puts "     hypertable write latency was #{write_latency}"
