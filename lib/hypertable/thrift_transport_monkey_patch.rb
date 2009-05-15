# Monkey patch for Thrift::FramedTransport typically found in:
# /usr/local/lib/ruby/site_ruby/1.8/thrift/transport/framed_transport.rb
#
# Rev 765279 (current recommended Thrift rev for Hypertable) does not
# reset buffer state when the connection is reopened.  This patch will
# be submitted to Thrift once it has been tested for a few weeks in 
# production.

module Thrift
  class FramedTransport < BaseTransport
    def initialize(transport, read=true, write=true)
      reset_state
      @transport = transport
      @read = read
      @write = write
    end

    def reset_state
      @rbuf = ''
      @wbuf = ''
      @index = 0
    end

    def open
      reset_state
      @transport.open
    end
  end
end
