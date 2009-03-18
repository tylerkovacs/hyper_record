# Monkey patch for Thrift::FramedTransport typically found in:
# /usr/local/lib/ruby/site_ruby/1.8/thrift/transport.rb
#
# Default implementation uses many, many calls to String.slice! for read
# buffer management.  ruby-prof shows that 99.5% of latency is spent
# here when processing relatively large responses.  This patch removes
# slice! usage by keeping the read buffer in the original string (@rbuf)
# and maintaining the current read position in a separate variable (@rpos).
# Reads are done by returning a substring from the current read position.
# Almost all overhead is eliminated.  Tests on relatively large queries
# show that execution time is very close to Hypertable CLI.

module Thrift
  class FramedTransport < Transport
    def initialize(transport, read=true, write=true)
      @transport = transport
      @rbuf      = ''
      @wbuf      = ''
      @read      = read
      @write     = write
      @rpos      = 0
    end

    def read(sz)
      return @transport.read(sz) unless @read

      return '' if sz <= 0

      read_frame if @rpos >= @rbuf.length

      @rpos += sz
      @rbuf[@rpos - sz, sz] || ''
    end

    def borrow(requested_length = 0)
      read_frame if @rpos >= @rbuf.length

      # there isn't any more coming, so if it's not enough, it's an error.
      raise EOFError if requested_length > (@rbuf.length - @rpos)

      @rbuf
    end

    def consume!(size)
      @rpos += size
      @rbuf[@rpos - size, size]
    end

    private

    def read_frame
      sz = @transport.read_all(4).unpack('N').first

      @rpos = 0
      @rbuf = @transport.read_all(sz)
    end
  end
end
