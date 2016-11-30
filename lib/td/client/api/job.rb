class TreasureData::API
module Job

  ####
  ## Job API
  ##

  # @param [Fixnum] from
  # @param [Fixnum] to
  # @param [String] status
  # @param [Hash] conditions
  # @return [Array]
  def list_jobs(from=0, to=nil, status=nil, conditions=nil)
    params = {}
    params['from'] = from.to_s if from
    params['to'] = to.to_s if to
    params['status'] = status.to_s if status
    params.merge!(conditions) if conditions
    code, body, res = get("/v3/job/list", params)
    if code != "200"
      raise_error("List jobs failed", res)
    end
    js = checked_json(body, %w[jobs])
    result = []
    js['jobs'].each {|m|
      job_id = m['job_id']
      type = (m['type'] || '?').to_sym
      database = m['database']
      status = m['status']
      query = m['query']
      start_at = m['start_at']
      end_at = m['end_at']
      cpu_time = m['cpu_time']
      result_size = m['result_size'] # compressed result size in msgpack.gz format
      result_url = m['result']
      priority = m['priority']
      retry_limit = m['retry_limit']
      duration = m['duration']
      num_records = m['num_records']
      result << [job_id, type, status, query, start_at, end_at, cpu_time,
                 result_size, result_url, priority, retry_limit, nil, database,
                 duration, num_records]
    }
    return result
  end

  # @param [String] job_id
  # @return [Array]
  def show_job(job_id)
    # use v3/job/status instead of v3/job/show to poll finish of a job
    code, body, res = get("/v3/job/show/#{e job_id}")
    if code != "200"
      raise_error("Show job failed", res)
    end
    js = checked_json(body, %w[status])
    # TODO debug
    type = (js['type'] || '?').to_sym  # TODO
    database = js['database']
    query = js['query']
    status = js['status']
    debug = js['debug']
    url = js['url']
    start_at = js['start_at']
    end_at = js['end_at']
    cpu_time = js['cpu_time']
    result_size = js['result_size'] # compressed result size in msgpack.gz format
    num_records = js['num_records']
    duration = js['duration']
    result = js['result'] # result target URL
    hive_result_schema = (js['hive_result_schema'] || '')
    if hive_result_schema.empty?
      hive_result_schema = nil
    else
      begin
        hive_result_schema = JSON.parse(hive_result_schema)
      rescue JSON::ParserError => e
        # this is a workaround for a Known Limitation in the Pig Engine which does not set a default, auto-generated
        #   column name for anonymous columns (such as the ones that are generated from UDF like COUNT or SUM).
        # The schema will contain 'nil' for the name of those columns and that breaks the JSON parser since it violates
        #   the JSON syntax standard.
        if type == :pig and hive_result_schema !~ /[\{\}]/
          begin
            # NOTE: this works because a JSON 2 dimensional array is the same as a Ruby one.
            #   Any change in the format for the hive_result_schema output may cause a syntax error, in which case
            #   this lame attempt at fixing the problem will fail and we will be raising the original JSON exception
            hive_result_schema = eval(hive_result_schema)
          rescue SyntaxError => ignored_e
            raise e
          end
          hive_result_schema.each_with_index {|col_schema, idx|
            if col_schema[0].nil?
              col_schema[0] = "_col#{idx}"
            end
          }
        else
          raise e
        end
      end
    end
    priority = js['priority']
    retry_limit = js['retry_limit']
    return [type, query, status, url, debug, start_at, end_at, cpu_time,
            result_size, result, hive_result_schema, priority, retry_limit, nil, database, duration, num_records]
  end

  # @param [String] job_id
  # @return [String] HTTP status
  def job_status(job_id)
    code, body, res = get("/v3/job/status/#{e job_id}")
    if code != "200"
      raise_error("Get job status failed", res)
    end

    js = checked_json(body, %w[status])
    return js['status']
  end

  # @param [String] job_id
  # @return [Array]
  def job_result(job_id)
    result = []
    unpacker = MessagePack::Unpacker.new
    job_result_download(job_id) do |chunk|
      unpacker.feed_each(chunk) do |row|
        result << row
      end
    end
    return result
  end

  # block is optional and must accept 1 parameter
  #
  # @param [String] job_id
  # @param [String] format
  # @param [IO] io
  # @param [Proc] block
  # @return [nil, String]
  def job_result_format(job_id, format, io=nil)
    if io
      job_result_download(job_id, format) do |chunk, total|
        io.write chunk
        yield total if block_given?
      end
      nil
    else
      body = String.new
      job_result_download(job_id, format) do |chunk|
        body << chunk
      end
      body
    end
  end

  # block is optional and must accept 1 argument
  #
  # @param [String] job_id
  # @param [Proc] block
  # @return [nil]
  def job_result_each(job_id, &block)
    upkr = MessagePack::Unpacker.new
    # default to decompressing the response since format is fixed to 'msgpack'
    job_result_download(job_id) do |chunk|
      upkr.feed_each(chunk, &block)
    end
    nil
  end

  # block is optional and must accept 1 argument
  #
  # @param [String] job_id
  # @param [Proc] block
  # @return [nil]
  def job_result_each_with_compr_size(job_id)
    upkr = MessagePack::Unpacker.new
    # default to decompressing the response since format is fixed to 'msgpack'
    job_result_download(job_id) do |chunk, total|
      upkr.feed_each(chunk) {|unpacked|
        yield unpacked, total if block_given?
      }
    end
    nil
  end

  # @param [String] job_id
  # @param [String] format
  # @return [String]
  def job_result_raw(job_id, format, io = nil)
    body = io ? nil : String.new
    job_result_download(job_id, format, false) do |chunk, total|
      if io
        io.write(chunk)
        yield total if block_given?
      else
        body << chunk
      end
    end
    body
  end

  # @param [String] job_id
  # @return [String]
  def kill(job_id)
    code, body, res = post("/v3/job/kill/#{e job_id}")
    if code != "200"
      raise_error("Kill job failed", res)
    end
    js = checked_json(body, %w[])
    former_status = js['former_status']
    return former_status
  end

  # @param [String] q
  # @param [String] db
  # @param [String] result_url
  # @param [Fixnum] priority
  # @param [Hash] opts
  # @return [String] job_id
  def hive_query(q, db=nil, result_url=nil, priority=nil, retry_limit=nil, opts={})
    query(q, :hive, db, result_url, priority, retry_limit, opts)
  end

  # @param [String] q
  # @param [String] db
  # @param [String] result_url
  # @param [Fixnum] priority
  # @param [Hash] opts
  # @return [String] job_id
  def pig_query(q, db=nil, result_url=nil, priority=nil, retry_limit=nil, opts={})
    query(q, :pig, db, result_url, priority, retry_limit, opts)
  end

  # @param [String] q
  # @param [Symbol] type
  # @param [String] db
  # @param [String] result_url
  # @param [Fixnum] priority
  # @param [Hash] opts
  # @return [String] job_id
  def query(q, type=:hive, db=nil, result_url=nil, priority=nil, retry_limit=nil, opts={})
    params = {'query' => q}.merge(opts)
    params['result'] = result_url if result_url
    params['priority'] = priority if priority
    params['retry_limit'] = retry_limit if retry_limit
    code, body, res = post("/v3/job/issue/#{type}/#{e db}", params)
    if code != "200"
      raise_error("Query failed", res)
    end
    js = checked_json(body, %w[job_id])
    return js['job_id'].to_s
  end

  private

  def validate_content_length_with_range(response, current_total_chunk_size)
    if expected_size = response.header['Content-Range'][0]
      expected_size = expected_size[/\d+$/].to_i
    elsif expected_size = response.header['Content-Length'][0]
      expected_size = expected_size.to_i
    end

    if expected_size.nil?
    elsif current_total_chunk_size < expected_size
      # too small
      # NOTE:
      #   ext/openssl raises EOFError in case where underlying connection
      #   causes an error, but httpclient ignores it.
      #   https://github.com/nahi/httpclient/blob/v3.2.8/lib/httpclient/session.rb#L1003
      raise EOFError, 'httpclient IncompleteError'
    elsif current_total_chunk_size > expected_size
      # too large
      raise_error("Get job result failed", response)
    end
  end

  def job_result_download(job_id, format='msgpack', autodecode=true)
    client, header = new_client
    client.send_timeout = @send_timeout
    client.receive_timeout = @read_timeout
    header['Accept-Encoding'] = 'deflate, gzip'

    url = build_endpoint("/v3/job/result/#{e job_id}", @host)
    params = {'format' => format}

    unless ENV['TD_CLIENT_DEBUG'].nil?
      puts "DEBUG: REST GET call:"
      puts "DEBUG:   header: " + header.to_s
      puts "DEBUG:   url:    " + url.to_s
      puts "DEBUG:   params: " + params.to_s
    end

    # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
    retry_delay = @retry_delay
    cumul_retry_delay = 0
    current_total_chunk_size = 0
    infl = nil
    begin # LOOP of Network/Server errors
      response = nil
      client.get(url, params, header) do |res, chunk|
        unless response
          case res.status
          when 200
            if current_total_chunk_size != 0
              # try to resume but the server returns 200
              raise_error("Get job result failed", res)
            end
          when 206 # resuming
          else
            if res.status/100 == 5 && cumul_retry_delay < @max_cumul_retry_delay
              $stderr.puts "Error #{res.status}: #{get_error(res)}. Retrying after #{retry_delay} seconds..."
              sleep retry_delay
              cumul_retry_delay += retry_delay
              retry_delay *= 2
              redo
            end
            raise_error("Get job result failed", res)
          end
          if infl.nil? && autodecode
            case res.header['Content-Encoding'][0].to_s.downcase
            when 'gzip'
              infl = Zlib::Inflate.new(Zlib::MAX_WBITS + 16)
            when 'deflate'
              infl = Zlib::Inflate.new
            end
          end
        end
        response = res
        current_total_chunk_size += chunk.bytesize
        chunk = infl.inflate(chunk) if infl
        yield chunk, current_total_chunk_size
      end

      # completed?
      validate_content_length_with_range(response, current_total_chunk_size)
    rescue Errno::ECONNREFUSED, Errno::ECONNRESET, Timeout::Error, EOFError, OpenSSL::SSL::SSLError, SocketError => e
      if response # at least a chunk is downloaded
        if etag = response.header['ETag'][0]
          header['If-Range'] = etag
          header['Range'] = "bytes=#{current_total_chunk_size}-"
        end
      end

      $stderr.print "#{e.class}: #{e.message}. "
      if cumul_retry_delay < @max_cumul_retry_delay
        $stderr.puts "Retrying after #{retry_delay} seconds..."
        sleep retry_delay
        cumul_retry_delay += retry_delay
        retry_delay *= 2
        retry
      end
      raise
    end

    unless ENV['TD_CLIENT_DEBUG'].nil?
      puts "DEBUG: REST GET response:"
      puts "DEBUG:   header: " + response.header.to_s
      puts "DEBUG:   status: " + response.code.to_s
      puts "DEBUG:   body:   " + response.body.to_s
    end

    nil
  ensure
    infl.close if infl
  end

  class NullInflate
    def inflate(chunk)
      chunk
    end

    def close
    end
  end

  def create_inflalte_or_null_inflate(response)
    if response.header['Content-Encoding'].empty?
      NullInflate.new
    else
      create_inflate(response)
    end
  end

  def create_inflate(response)
    if response.header['Content-Encoding'].include?('gzip')
      Zlib::Inflate.new(Zlib::MAX_WBITS + 16)
    else
      Zlib::Inflate.new
    end
  end
end
end
