
module TreasureData


class APIError < StandardError
end

class AuthError < APIError
end

class AlreadyExistsError < APIError
end

class NotFoundError < APIError
end


class API
  def initialize(apikey, opts={})
    require 'json'
    require 'time'
    require 'uri'
    @apikey = apikey

    endpoint = opts[:endpoint] || ENV['TD_API_SERVER'] || 'api.treasure-data.com'
    uri = URI.parse(endpoint)

    case uri.scheme
    when 'http', 'https'
      @host = uri.host
      @port = uri.port
      @ssl = uri.scheme == 'https'
      @base_path = uri.path.to_s

    else
      if uri.port
        # invalid URI
        raise "Invalid endpoint: #{endpoint}"
      end

      # generic URI
      @host, @port = endpoint.split(':', 2)
      @port = @port.to_i
      if opts[:ssl]
        @port = 443 if @port == 0
        @ssl = true
      else
        @port = 80 if @port == 0
        @ssl = false
      end
      @base_path = ''
    end
  end

  # TODO error check & raise appropriate errors

  attr_reader :apikey

  def self.validate_database_name(name)
    name = name.to_s
    if name.empty?
      raise "Empty name is not allowed"
    end
    if name.length < 3 || 32 < name.length
      raise "Name must be 3 to 32 characters, got #{name.length} characters."
    end
    unless name =~ /^([a-z0-9_]+)$/
      raise "Name must consist only of alphabets, numbers, '_'."
    end
    name
  end

  def self.validate_table_name(name)
    validate_database_name(name)
  end

  def self.validate_result_set_name(name)
    validate_database_name(name)
  end

  def self.validate_column_name(name)
    name = name.to_s
    if name.empty?
      raise "Empty column name is not allowed"
    end
    if 32 < name.length
      raise "Column name must be to 32 characters, got #{name.length} characters."
    end
    unless name =~ /^([a-z0-9_]+)$/
      raise "Column name must consist only of alphabets, numbers, '_'."
    end
  end

  def self.normalize_database_name(name)
    name = name.to_s
    if name.empty?
      raise "Empty name is not allowed"
    end
    if name.length < 3
      name += "_"*(3-name.length)
    end
    if 32 < name.length
      name = name[0,30]+"__"
    end
    name = name.downcase
    name = name.gsub(/[^a-z0-9_]/, '_')
    name
  end

  def self.normalize_table_name(name)
    normalize_database_name(name)
  end

  # TODO support array types
  def self.normalize_type_name(name)
    case name
    when /int/i, /integer/i
      "int"
    when /long/i, /bigint/i
      "long"
    when /string/i
      "string"
    when /float/i
      "float"
    when /double/i
      "double"
    else
      raise "Type name must eather of int, long, string float or double"
    end
  end

  ####
  ## Database API
  ##

  # => [name:String]
  def list_databases
    code, body, res = get("/v3/database/list")
    if code != "200"
      raise_error("List databases failed", res)
    end
    js = checked_json(body, %w[databases])
    names = js["databases"].map {|dbinfo| dbinfo['name'] }
    return names
  end

  # => true
  def delete_database(db)
    code, body, res = post("/v3/database/delete/#{e db}")
    if code != "200"
      raise_error("Delete database failed", res)
    end
    return true
  end

  # => true
  def create_database(db)
    code, body, res = post("/v3/database/create/#{e db}")
    if code != "200"
      raise_error("Create database failed", res)
    end
    return true
  end


  ####
  ## Table API
  ##

  # => {name:String => [type:Symbol, count:Integer]}
  def list_tables(db)
    code, body, res = get("/v3/table/list/#{e db}")
    if code != "200"
      raise_error("List tables failed", res)
    end
    js = checked_json(body, %w[tables])
    result = {}
    js["tables"].map {|m|
      name = m['name']
      type = (m['type'] || '?').to_sym
      count = (m['count'] || 0).to_i  # TODO?
      schema = JSON.parse(m['schema'] || '[]')
      result[name] = [type, schema, count]
    }
    return result
  end

  def create_log_or_item_table(db, table, type)
    code, body, res = post("/v3/table/create/#{e db}/#{e table}/#{type}")
    if code != "200"
      raise_error("Create #{type} table failed", res)
    end
    return true
  end
  private :create_log_or_item_table

  # => true
  def create_log_table(db, table)
    create_table(db, table, :log)
  end

  # => true
  def create_item_table(db, table)
    create_table(db, table, :item)
  end

  def create_table(db, table, type)
    schema = schema.to_s
    code, body, res = post("/v3/table/create/#{e db}/#{e table}/#{type}")
    if code != "200"
      raise_error("Create #{type} table failed", res)
    end
    return true
  end
  private :create_table

  # => true
  def update_schema(db, table, schema_json)
    code, body, res = post("/v3/table/update-schema/#{e db}/#{e table}", {'schema'=>schema_json})
    if code != "200"
      raise_error("Create schema table failed", res)
    end
    return true
  end

  # => type:Symbol
  def delete_table(db, table)
    code, body, res = post("/v3/table/delete/#{e db}/#{e table}")
    if code != "200"
      raise_error("Drop table failed", res)
    end
    js = checked_json(body, %w[])
    type = (js['type'] || '?').to_sym
    return type
  end

  def tail(db, table, count, to, from, &block)
    params = {'format' => 'msgpack'}
    params['count'] = count.to_s if count
    params['to'] = to.to_s if to
    params['from'] = from.to_s if from
    code, body, res = get("/v3/table/tail/#{e db}/#{e table}", params)
    if code != "200"
      raise_error("Tail table failed", res)
    end
    require 'msgpack'
    if block
      MessagePack::Unpacker.new.feed_each(body, &block)
      nil
    else
      result = []
      MessagePack::Unpacker.new.feed_each(body) {|row|
        result << row
      }
      return result
    end
  end


  ####
  ## Job API
  ##

  # => [(jobId:String, type:Symbol, status:String, start_at:String, end_at:String, rset:String)]
  def list_jobs(from=0, to=nil)
    params = {}
    params['from'] = from.to_s if from
    params['to'] = to.to_s if to
    code, body, res = get("/v3/job/list", params)
    if code != "200"
      raise_error("List jobs failed", res)
    end
    js = checked_json(body, %w[jobs])
    result = []
    js['jobs'].each {|m|
      job_id = m['job_id']
      type = (m['type'] || '?').to_sym
      status = m['status']
      query = m['query']
      start_at = m['start_at']
      end_at = m['end_at']
      rset = m['result']
      result << [job_id, type, status, query, start_at, end_at, rset]
    }
    return result
  end

  # => (type:Symbol, status:String, result:String, url:String, result:String)
  def show_job(job_id)
    code, body, res = get("/v3/job/show/#{e job_id}")
    if code != "200"
      raise_error("Show job failed", res)
    end
    js = checked_json(body, %w[status])
    # TODO debug
    type = (js['type'] || '?').to_sym  # TODO
    query = js['query']
    status = js['status']
    debug = js['debug']
    url = js['url']
    start_at = js['start_at']
    end_at = js['end_at']
    result = js['result']
    return [type, query, status, url, debug, start_at, end_at, result]
  end

  def job_result(job_id)
    require 'msgpack'
    code, body, res = get("/v3/job/result/#{e job_id}", {'format'=>'msgpack'})
    if code != "200"
      raise_error("Get job result failed", res)
    end
    result = []
    MessagePack::Unpacker.new.feed_each(body) {|row|
      result << row
    }
    return result
  end

  def job_result_format(job_id, format)
    # TODO chunked encoding
    code, body, res = get("/v3/job/result/#{e job_id}", {'format'=>format})
    if code != "200"
      raise_error("Get job result failed", res)
    end
    return body
  end

  def job_result_each(job_id, &block)
    # TODO chunked encoding
    require 'msgpack'
    code, body, res = get("/v3/job/result/#{e job_id}", {'format'=>'msgpack'})
    if code != "200"
      raise_error("Get job result failed", res)
    end
    result = []
    MessagePack::Unpacker.new.feed_each(body) {|row|
      yield row
    }
    nil
  end

  def job_result_raw(job_id, format)
    code, body, res = get("/v3/job/result/#{e job_id}", {'format'=>format})
    if code != "200"
      raise_error("Get job result failed", res)
    end
    return body
  end

  def kill(job_id)
    code, body, res = post("/v3/job/kill/#{e job_id}")
    if code != "200"
      raise_error("Get job result failed", res)
    end
    js = checked_json(body, %w[])
    former_status = js['former_status']
    return former_status
  end

  # => jobId:String
  def hive_query(q, db=nil, rset=nil)
    params = {'query' => q}
    params['result'] = rset if rset
    code, body, res = post("/v3/job/issue/hive/#{e db}", params)
    if code != "200"
      raise_error("Query failed", res)
    end
    js = checked_json(body, %w[job_id])
    return js['job_id'].to_s
  end


  ####
  ## Schedule API
  ##

  # => start:String
  def create_schedule(name, opts)
    params = opts.update({'type'=>'hive'})
    code, body, res = post("/v3/schedule/create/#{e name}", params)
    if code != "200"
      raise_error("Create schedule failed", res)
    end
    js = checked_json(body, %w[start])
    return js['start']
  end

  # => cron:String, query:String
  def delete_schedule(name)
    code, body, res = post("/v3/schedule/delete/#{e name}")
    if code != "200"
      raise_error("Delete schedule failed", res)
    end
    js = checked_json(body, %w[])
    return js['cron'], js["query"]
  end

  # => [(name:String, cron:String, query:String, database:String, rset:String)]
  def list_schedules
    code, body, res = get("/v3/schedule/list")
    if code != "200"
      raise_error("List schedules failed", res)
    end
    js = checked_json(body, %w[schedules])
    result = []
    js['schedules'].each {|m|
      name = m['name']
      cron = m['cron']
      query = m['query']
      database = m['database']
      rset = m['result']
      timezone = m['timezone']
      delay = m['delay']
      next_time = m['next_time']
      result << [name, cron, query, database, rset, timezone, delay, next_time]
    }
    return result
  end

  def update_schedule(name, params)
    code, body, res = get("/v3/schedule/update/#{e name}", params)
    if code != "200"
      raise_error("Update schedule failed", res)
    end
    return nil
  end

  def history(name, from=0, to=nil)
    params = {}
    params['from'] = from.to_s if from
    params['to'] = to.to_s if to
    code, body, res = get("/v3/schedule/history/#{e name}", params)
    if code != "200"
      raise_error("List history failed", res)
    end
    js = checked_json(body, %w[history])
    result = []
    js['history'].each {|m|
      job_id = m['job_id']
      type = (m['type'] || '?').to_sym
      status = m['status']
      query = m['query']
      start_at = m['start_at']
      end_at = m['end_at']
      scheduled_at = m['scheduled_at']
      rset = m['result']
      result << [scheduled_at, job_id, type, status, query, start_at, end_at, rset]
    }
    return result
  end

  def run_schedule(name, time, num)
    params = {}
    params = {'num' => num} if num
    code, body, res = post("/v3/schedule/run/#{e name}/#{e time}", params)
    if code != "200"
      raise_error("Run schedule failed", res)
    end
    js = checked_json(body, %w[jobs])
    result = []
    js['jobs'].each {|m|
      job_id = m['job_id']
      scheduled_at = m['scheduled_at']
      result << [job_id, scheduled_at]
    }
    return result
  end

  ####
  ## Import API
  ##

  # => time:Float
  def import(db, table, format, stream, size)
    code, body, res = put("/v3/table/import/#{e db}/#{e table}/#{format}", stream, size)
    if code[0] != ?2
      raise_error("Import failed", res)
    end
    js = checked_json(body, %w[])
    time = js['time'].to_f
    return time
  end


  ####
  ## Result set API
  ##

  # => (
  #   info:(type:String, host:String, port:Integer, database:String, user:String, pass:String)
  #   entries:[name:String]
  # )
  def list_result_set
    code, body, res = get("/v3/result/list")
    if code != "200"
      raise_error("List result set failed", res)
    end
    js = checked_json(body, %w[host port user pass])
    type = (js["type"] || 'unknown').to_s
    host = js["host"].to_s
    port = js["port"].to_i
    database = js["database"].to_s
    user = js["user"].to_s
    pass = js["pass"].to_s
    names = (js["results"] || []).map {|rsets|
      rsets['name'].to_s
    }
    info = [type, host, port, database, user, pass]
    return info, names
  end

  # => true
  def create_result_set(db)
    code, body, res = post("/v3/result/create/#{e db}")
    if code != "200"
      raise_error("Create result set failed", res)
    end
    return true
  end

  # => true
  def delete_result_set(db)
    code, body, res = post("/v3/result/delete/#{e db}")
    if code != "200"
      raise_error("Delete result set failed", res)
    end
    return true
  end


  ####
  ## Aggregation Schema API
  ##

  # => [(name:String, relation_key:String)]
  def list_aggregation_schema
    code, body, res = get("/v3/aggr/list")
    if code != "200"
      raise_error("List aggregation schema failed", res)
    end
    js = checked_json(body, %w[aggrs])
    result = js["aggrs"].map {|aggrinfo|
      name = aggrinfo['name'].to_s
      relation_key = aggrinfo['relation_key'].to_s
      timezone = aggrinfo['timezone'].to_s
      [name, relation_key, timezone]
    }
    return result
  end

  # => true
  def create_aggregation_schema(name, relation_key, params={})
    params['relation_key'] = relation_key if relation_key
    code, body, res = post("/v3/aggr/create/#{e name}", params)
    if code != "200"
      raise_error("Create aggregation schema failed", res)
    end
    return true
  end

  # => true
  def delete_aggregation_schema(name)
    code, body, res = post("/v3/aggr/delete/#{e name}")
    if code != "200"
      raise_error("Delete aggregation schema failed", res)
    end
    return true
  end

  # => [
  #   {
  #     relation_key: String,
  #     logs: (entry_name:String, comment:String, database:String, table:String,
  #            os:Array[String], value_key:String?, count_key:String?),
  #     attrs: (entry_name:String, comment:String, database:String, table:String,
  #            method_name:String, parameters:Hash[String=>Object])
  #   }
  # ]
  def show_aggregation_schema(name)
    code, body, res = get("/v3/aggr/show/#{e name}")
    if code != "200"
      raise_error("Show job failed", res)
    end
    js = checked_json(body, %w[relation_key logs attrs])
    relation_key = js['relation_key']
    logs = js['logs'].map {|loginfo|
      entry_name = loginfo['name'].to_s
      comment = loginfo['comment'].to_s
      database = loginfo['database'].to_s
      table = loginfo['table'].to_s
      os = loginfo['os']
      value_key = loginfo['value_key'].to_s
      count_key = loginfo['count_key'].to_s
      value_key = nil if value_key.empty?
      count_key = nil if count_key.empty?
      [entry_name, comment, database, table, os, value_key, count_key]
    }
    attrs = js['attrs'].map {|attrinfo|
      entry_name = attrinfo['name'].to_s
      comment = attrinfo['comment'].to_s
      database = attrinfo['database'].to_s
      table = attrinfo['table'].to_s
      method_name = attrinfo['method_name'].to_s
      parameters = attrinfo['parameters'].to_s
      parameters = "{}" if parameters.empty?
      parameters = JSON.parse(parameters)
      [entry_name, comment, database, table, method_name, parameters]
    }
    return [relation_key, logs, attrs]
  end

  # => true
  def create_aggregation_log_entry(name, entry_name, comment, db, table, okeys, value_key, count_key)
    params = {}
    params['comment'] = comment if comment
    okeys.each_with_index {|okey,i|
      params["os[#{i}]"] = okey
    }
    params['value_key'] = value_key if value_key
    params['count_key'] = count_key if count_key
    code, body, res = post("/v3/aggr/entry/log/create/#{e name}/#{e entry_name}/#{e db}/#{e table}", params)
    if code != "200"
      raise_error("Create aggregation log entry failed", res)
    end
    return true
  end

  # => true
  def delete_aggregation_log_entry(name, entry_name)
    code, body, res = post("/v3/aggr/entry/log/delete/#{e name}/#{e entry_name}")
    if code != "200"
      raise_error("Delete aggregation log entry failed", res)
    end
    return true
  end

  # => true
  def create_aggregation_attr_entry(name, entry_name, comment, db, table, method_name, parameters)
    params = {}
    parameters.each_pair {|k,v|
      params["parameters[#{k}]"] = v.to_s
    }
    params['comment'] = comment if comment
    code, body, res = post("/v3/aggr/entry/attr/create/#{e name}/#{e entry_name}/#{e db}/#{e table}/#{e method_name}", params)
    if code != "200"
      raise_error("Create aggregation attr entry failed", res)
    end
    return true
  end

  # => true
  def delete_aggregation_attr_entry(name, entry_name)
    code, body, res = post("/v3/aggr/entry/attr/delete/#{e name}/#{e entry_name}")
    if code != "200"
      raise_error("Delete aggregation attr entry failed", res)
    end
    return true
  end


  ####
  ## User API
  ##

  # apikey:String
  def authenticate(user, password)
    code, body, res = post("/v3/user/authenticate", {'user'=>user, 'password'=>password})
    if code != "200"
      if code == "400"
        raise_error("Authentication failed", res, AuthError)
      else
        raise_error("Authentication failed", res)
      end
    end
    js = checked_json(body, %w[apikey])
    apikey = js['apikey']
    return apikey
  end

  ####
  ## Server Status API
  ##

  # => status:String
  def server_status
    code, body, res = get('/v3/system/server_status')
    if code != "200"
      return "Server is down (#{code})"
    end
    js = checked_json(body, %w[status])
    status = js['status']
    return status
  end

  private

  def get(url, params=nil)
    http, header = new_http

    path = @base_path + url
    if params && !params.empty?
      path << "?"+params.map {|k,v|
        "#{k}=#{e v}"
      }.join('&')
    end

    request = Net::HTTP::Get.new(path, header)

    response = http.request(request)
    return [response.code, response.body, response]
  end

  def post(url, params=nil)
    http, header = new_http

    path = @base_path + url

    if params && !params.empty?
      request = Net::HTTP::Post.new(path, header)
      request.set_form_data(params)
    else
      header['Content-Length'] = 0.to_s
      request = Net::HTTP::Post.new(path, header)
    end

    response = http.request(request)
    return [response.code, response.body, response]
  end

  def put(url, stream, size)
    http, header = new_http

    http.read_timeout = 600

    path = @base_path + url

    header['Content-Type'] = 'application/octet-stream'
    header['Content-Length'] = size.to_s

    request = Net::HTTP::Put.new(url, header)
    if stream.class.name == 'StringIO'
      request.body = stream.string
    else
      if request.respond_to?(:body_stream=)
        request.body_stream = stream
      else  # Ruby 1.8
        request.body = stream.read
      end
    end

    response = http.request(request)
    return [response.code, response.body, response]
  end

  def new_http
    require 'net/http'
    require 'net/https'
    require 'time'

    http = Net::HTTP.new(@host, @port)
    if @ssl
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_PEER
      store = OpenSSL::X509::Store.new
      http.cert_store = store
    end

    header = {}
    if @apikey
      header['Authorization'] = "TD1 #{apikey}"
    end
    header['Date'] = Time.now.rfc2822

    return http, header
  end

  def raise_error(msg, res, klass=nil)
    begin
      js = JSON.load(res.body)
      msg = js['message']
      error_code = js['error_code']

      if klass
        raise klass, "#{error_code}: #{msg}"
      elsif res.code == "404"
        raise NotFoundError, "#{error_code}: #{msg}"
      elsif res.code == "409"
        raise AlreadyExistsError, "#{error_code}: #{msg}"
      else
        raise APIError, "#{error_code}: #{msg}"
      end

    rescue
      if klass
        raise klass, "#{error_code}: #{msg}"
      elsif res.code == "404"
        raise NotFoundError, "#{msg}: #{res.body}"
      elsif res.code == "409"
        raise AlreadyExistsError, "#{msg}: #{res.body}"
      else
        raise APIError, "#{msg}: #{res.body}"
      end
    end
    # TODO error
  end

  def e(s)
    require 'cgi'
    CGI.escape(s.to_s)
  end

  def checked_json(body, required)
    js = nil
    begin
      js = JSON.load(body)
    rescue
      raise "Unexpected API response: #{$!}"
    end
    unless js.is_a?(Hash)
      raise "Unexpected API response: #{body}"
    end
    required.each {|k|
      unless js[k]
        raise "Unexpected API response: #{body}"
      end
    }
    js
  end
end


end

