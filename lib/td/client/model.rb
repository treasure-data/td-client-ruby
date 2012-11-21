
module TreasureData


class Model
  def initialize(client)
    @client = client
  end

  attr_reader :client
end

class Account < Model
  def initialize(client, account_id, plan, storage_size=nil, guaranteed_cores=nil, maximum_cores=nil, created_at=nil)
    super(client)
    @account_id = account_id
    @plan = plan
    @storage_size = storage_size
    @guaranteed_cores = guaranteed_cores
    @maximum_cores = maximum_cores
    @created_at = created_at
  end

  attr_reader :account_id, :plan, :storage_size, :guaranteed_cores, :maximum_cores

  def created_at
    @created_at && !@created_at.empty? ? Time.parse(@created_at) : nil
  end

  def storage_size_string
    if @storage_size <= 1024*1024
      return "0.0 GB"
    elsif @storage_size <= 60*1024*1024
      return "0.01 GB"
    elsif @storage_size <= 60*1024*1024*1024
      "%.1f GB" % (@storage_size.to_f / (1024*1024*1024))
    else
      "%d GB" % (@storage_size.to_f / (1024*1024*1024)).to_i
    end
  end
end

class Database < Model
  def initialize(client, db_name, tables=nil, count=nil, created_at=nil, updated_at=nil, org_name=nil)
    super(client)
    @db_name = db_name
    @tables = tables
    @count = count
    @created_at = created_at
    @updated_at = updated_at
    @org_name = org_name
  end

  attr_reader :org_name

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

  attr_reader :count

  def created_at
    @created_at && !@created_at.empty? ? Time.parse(@created_at) : nil
  end

  def updated_at
    @updated_at && !@updated_at.empty? ? Time.parse(@updated_at) : nil
  end

  def update_tables!
    @tables = @client.tables(@db_name)
  end
end

class Table < Model
  def initialize(client, db_name, table_name, type, schema, count, created_at=nil, updated_at=nil, estimated_storage_size=nil)
    super(client)
    @db_name = db_name
    @table_name = table_name
    @type = type
    @schema = schema
    @count = count
    @created_at = created_at
    @updated_at = updated_at
    @estimated_storage_size = estimated_storage_size
  end

  attr_reader :type, :db_name, :table_name, :schema, :count, :estimated_storage_size

  alias database_name db_name
  alias name table_name

  def created_at
    @created_at && !@created_at.empty? ? Time.parse(@created_at) : nil
  end

  def updated_at
    @updated_at && !@updated_at.empty? ? Time.parse(@updated_at) : nil
  end

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

  def estimated_storage_size_string
    if @estimated_storage_size <= 1024*1024
      return "0.0 GB"
    elsif @estimated_storage_size <= 60*1024*1024
      return "0.01 GB"
    elsif @estimated_storage_size <= 60*1024*1024*1024
      "%.1f GB" % (@estimated_storage_size.to_f / (1024*1024*1024))
    else
      "%d GB" % (@estimated_storage_size.to_f / (1024*1024*1024)).to_i
    end
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

  def initialize(client, job_id, type, query, status=nil, url=nil, debug=nil, start_at=nil, end_at=nil, result=nil, result_url=nil, hive_result_schema=nil, priority=nil, org_name=nil, db_name=nil)
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
    @result_url = result_url
    @hive_result_schema = hive_result_schema
    @priority = priority
    @org_name = org_name
    @db_name = db_name
  end

  attr_reader :job_id, :type, :result_url
  attr_reader :hive_result_schema, :priority, :org_name, :db_name

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

  def result
    unless @result
      return nil unless finished?
      @result = @client.job_result(@job_id)
    end
    @result
  end

  def result_format(format, io=nil)
    return nil unless finished?
    @client.job_result_format(@job_id, format, io)
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
    query, status, url, debug, start_at, end_at, result_url, hive_result_schema, priority, org_name, db_name = @client.job_status(@job_id)
    @query = query
    @status = status
    @url = url
    @debug = debug
    @start_at = start_at
    @end_at = end_at
    @hive_result_schema = hive_result_schema
    @org_name = org_name
    @db_name = db_name
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
  def initialize(client, name, cron, query, database=nil, result_url=nil, timezone=nil, delay=nil, next_time=nil, priority=nil, org_name=nil)
    super(client)
    @name = name
    @cron = cron
    @query = query
    @database = database
    @result_url = result_url
    @timezone = timezone
    @delay = delay
    @next_time = next_time
    @priority = priority
    @org_name = org_name
  end

  attr_reader :name, :cron, :query, :database, :result_url, :timezone, :delay, :priority, :org_name

  def next_time
    @next_time ? Time.parse(@next_time) : nil
  end

  def run(time, num)
    @client.run_schedule(time, num)
  end
end


class Result < Model
  def initialize(client, name, url, org_name)
    super(client)
    @name = name
    @url = url
    @org_name = org_name
  end

  attr_reader :name, :url, :org_name
end


class BulkImport < Model
  def initialize(client, data={})
    super(client)
    @name = data['name']
    @database = data['database']
    @table = data['table']
    @status = data['status']
    @upload_frozen = data['upload_frozen']
    @job_id = data['job_id']
    @valid_records = data['valid_records']
    @error_records = data['error_records']
    @valid_parts = data['valid_parts']
    @error_parts = data['error_parts']
    @org_name = data['organization']
  end

  attr_reader :name
  attr_reader :database
  attr_reader :table
  attr_reader :status
  attr_reader :job_id
  attr_reader :valid_records
  attr_reader :error_records
  attr_reader :valid_parts
  attr_reader :error_parts
  attr_reader :org_name

  def upload_frozen?
    @upload_frozen
  end
end


class Organization < Model
  def initialize(client, name)
    super(client)
    @name = name
  end

  attr_reader :client, :name
end


class Role < Model
  def initialize(client, name, org_name, user_names)
    super(client)
    @name = name
    @org_name = org_name
    @user_names = user_names
  end

  attr_reader :client, :name, :org_name, :user_names
end


class User < Model
  def initialize(client, name, org_name, role_names, email)
    super(client)
    @name = name
    @org_name = org_name
    @role_names = role_names
    @email = email
  end

  attr_reader :client, :name, :org_name, :role_names, :email
end


class AccessControl < Model
  def initialize(client, subject, action, scope, grant_option)
    super(client)
    @subject = subject
    @action = action
    @scope = scope
    @grant_option = grant_option
  end

  attr_reader :subject, :action, :scope, :grant_option
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

