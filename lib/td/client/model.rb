
module TreasureData


class Model
  def initialize(client)
    @client = client
  end

  attr_reader :client
end

class Database < Model
  def initialize(client, db_name, tables=nil)
    super(client)
    @db_name = db_name
    @tables = tables
  end

  def name
    @db_name
  end

  def tables
    update_tables! unless @tables
    @tables
  end

  def create_log_table(name)
    @client.create_log_table(@db_name, name)
  end

  def create_item_table(name)
    @client.create_item_table(@db_name, name)
  end

  def table(table_name)
    @client.table(@db_name, table_name)
  end

  def delete
    @client.delete_database(@db_name)
  end

  def query(q)
    @client.query(@db_name, q)
  end

  def update_tables!
    @tables = @client.tables(@db_name)
  end
end

class Table < Model
  def initialize(client, db_name, table_name, type, schema, count)
    super(client)
    @db_name = db_name
    @table_name = table_name
    @type = type
    @schema = schema
    @count = count
  end

  attr_reader :type, :db_name, :table_name, :schema, :count

  alias database_name db_name
  alias name table_name

  def database
    @client.database(@db_name)
  end

  def identifier
    "#{@db_name}.#{@table_name}"
  end

  def delete
    @client.delete_table(@db_name, @table_name)
  end

  def tail(count, to=nil, from=nil)
    @client.tail(@db_name, @table_name, count, to, from)
  end

  def import(format, stream, size)
    @client.import(@db_name, @table_name, format, stream, size)
  end

  def export(storage_type, opts={})
    @client.export(@db_name, @table_name, storage_type, opts)
  end
end

class Schema
  class Field
    def initialize(name, type)
      @name = name
      @type = type
    end
    attr_reader :name
    attr_reader :type
  end

  def self.parse(cols)
    fields = cols.split(',').map {|col|
      name, type, *_ = col.split(':')
      Field.new(name, type)
    }
    Schema.new(fields)
  end

  def initialize(fields=[])
    @fields = fields
  end

  attr_reader :fields

  def add_field(name, type)
    @fields << Field.new(name, type)
  end

  def merge(schema)
    nf = @fields.dup
    schema.fields.each {|f|
      if i = nf.find_index {|sf| sf.name == f.name }
        nf[i] = f
      else
        nf << f
      end
    }
    Schema.new(nf)
  end

  def to_json(*args)
    @fields.map {|f| [f.name, f.type] }.to_json(*args)
  end

  def from_json(obj)
    @fields = obj.map {|f|
      Field.new(f[0], f[1])
    }
    self
  end
end

class Job < Model
  STATUS_QUEUED = "queued"
  STATUS_BOOTING = "booting"
  STATUS_RUNNING = "running"
  STATUS_SUCCESS = "success"
  STATUS_ERROR = "error"
  STATUS_KILLED = "killed"
  FINISHED_STATUS = [STATUS_SUCCESS, STATUS_ERROR, STATUS_KILLED]

  def initialize(client, job_id, type, query, status=nil, url=nil, debug=nil, start_at=nil, end_at=nil, result=nil, rset=nil)
    super(client)
    @job_id = job_id
    @type = type
    @url = url
    @query = query
    @status = status
    @debug = debug
    @start_at = start_at
    @end_at = end_at
    @result = result
    @rset = rset
  end

  attr_reader :job_id, :type, :rset

  def wait(timeout=nil)
    # TODO
  end

  def kill!
    # TODO
  end

  def query
    update_status! unless @query
    @query
  end

  def status
    update_status! unless @status
    @status
  end

  def url
    update_status! unless @url
    @url
  end

  def debug
    update_status! unless @debug
    @debug
  end

  def start_at
    update_status! unless @start_at
    @start_at && !@start_at.empty? ? Time.parse(@start_at) : nil
  end

  def end_at
    update_status! unless @end_at
    @end_at && !@end_at.empty? ? Time.parse(@end_at) : nil
  end

  def rset_name
    @rset ? @rset.name : nil
  end

  def result
    unless @result
      return nil unless finished?
      @result = @client.job_result(@job_id)
    end
    @result
  end

  def result_format(format)
    return nil unless finished?
    @client.job_result_format(@job_id, format)
  end

  def result_each(&block)
    if @result
      @result.each(&block)
    else
      @client.job_result_each(@job_id, &block)
    end
    nil
  end

  def finished?
    update_status! unless @status
    if FINISHED_STATUS.include?(@status)
      return true
    else
      return false
    end
  end

  def running?
    !finished?
  end

  def success?
    update_status! unless @status
    @status == "success"
  end

  def error?
    update_status! unless @status
    @status == "error"
  end

  def killed?
    update_status! unless @status
    @status == "killed"
  end

  def update_status!
    query, status, url, debug, start_at, end_at, rset = @client.job_status(@job_id)
    @query = query
    @status = status
    @url = url
    @debug = debug
    @start_at = start_at
    @end_at = end_at
    self
  end
end


class ScheduledJob < Job
  def initialize(client, scheduled_at, *super_args)
    super(client, *super_args)
    @scheduled_at = scheduled_at
  end

  def scheduled_at
    @scheduled_at ? Time.parse(@scheduled_at) : nil
  end
end


class Schedule < Model
  def initialize(client, name, cron, query, database=nil, rset=nil, timezone=nil, delay=nil, next_time=nil)
    super(client)
    @name = name
    @cron = cron
    @query = query
    @database = database
    @rset = rset
    @timezone = timezone
    @delay = delay
    @next_time = next_time
  end

  def rset_name
    @rset ? @rset.name : nil
  end

  attr_reader :name, :cron, :query, :database, :rset, :timezone, :delay

  def next_time
    @next_time ? Time.parse(@next_time) : nil
  end

  def run(time, num)
    @client.run_schedule(time, num)
  end
end


class ResultSetInfo < Model
  def initialize(client, type, host, port, database, user, password)
    super(client)
    @type = type
    @host = host
    @port = port
    @database = database
    @user = user
    @password = password
  end

  attr_reader :type, :host, :port, :database, :user, :password
end


class ResultSet < Model
  def initialize(client, name)
    super(client)
    @name = name
  end

  attr_reader :name
end


class AggregationSchema < Model
  def initialize(client, name, relation_key, logs=nil, attributes=nil, timezone=nil)
    super(client)
    @name = name
    @relation_key = relation_key
    @logs = logs
    @attributes = attributes
    @timezone = timezone
  end

  attr_reader :name, :relation_key, :timezone

  def logs
    update_entries! unless @logs
    @logs
  end

  def attributes
    update_entries! unless @attributes
    @attributes
  end

  def update_entries!
    sc = @client.aggregation_schema(@name)
    @relation_key = sc.relation_key
    @logs = sc.logs
    @attributes = sc.attributes
    self
  end
end


class LogAggregationSchemaEntry < Model
  def initialize(client, name, comment, table, okeys, value_key, count_key)
    super(client)
    @name = name
    @comment = comment
    @table = table
    @okeys = okeys
    @value_key = value_key
    @count_key = count_key
  end

  attr_reader :name, :comment, :table
  attr_reader :okeys, :value_key, :count_key
end


class AttributeAggregationSchemaEntry < Model
  def initialize(client, name, comment, table, method_name, parameters)
    super(client)
    @name = name
    @comment = comment
    @table = table
    @method_name = method_name
    @parameters = parameters
  end

  attr_reader :name, :comment, :table
  attr_reader :method_name, :parameters
end


end

