
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
    if @storage_size <= 1024 * 1024
      return "0.0 GB"
    elsif @storage_size <= 60 * 1024 * 1024
      return "0.01 GB"
    elsif @storage_size <= 60 * 1024 * 1024 * 1024
      "%.1f GB" % (@storage_size.to_f / (1024 * 1024 * 1024))
    else
      "%d GB" % (@storage_size.to_f / (1024 * 1024 * 1024)).to_i
    end
  end
end

class Database < Model
  PERMISSIONS = [:administrator, :full_access, :import_only, :query_only]
  PERMISSION_LIST_TABLES = [:administrator, :full_access]

  def initialize(client, db_name, tables=nil, count=nil, created_at=nil, updated_at=nil, org_name=nil, permission=nil)
    super(client)
    @db_name = db_name
    @tables = tables
    @count = count
    @created_at = created_at
    @updated_at = updated_at
    @permission = permission.to_sym
  end

  attr_reader :org_name, :permission, :count

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

  def created_at
    @created_at && !@created_at.empty? ? Time.parse(@created_at) : nil
  end

  def updated_at
    @updated_at && !@updated_at.empty? ? Time.parse(@updated_at) : nil
  end

  def update_tables!
    @tables = @client.tables(@db_name)
    # provide Table objects with a reference to the parent Database to avoid
    # requesting the Database information (such as permission) every time
    @tables.each {|table|
      table.database = self
    }
  end

end

class Table < Model
  def initialize(client, db_name, table_name, type, schema, count, created_at=nil, updated_at=nil, estimated_storage_size=nil, last_import=nil, last_log_timestamp=nil, expire_days=nil, primary_key=nil, primary_key_type=nil)
    super(client)
    @database = nil
    @db_name = db_name
    @table_name = table_name
    @type = type
    @schema = schema
    @count = count
    @created_at = created_at
    @updated_at = updated_at
    @estimated_storage_size = estimated_storage_size
    @last_import = last_import
    @last_log_timestamp = last_log_timestamp
    @expire_days = expire_days
    @primary_key = primary_key
    @primary_key_type = primary_key_type
  end

  attr_reader :type, :db_name, :table_name, :schema, :count, :estimated_storage_size, :primary_key, :primary_key_type

  alias database_name db_name
  alias name table_name

  def database=(database)
    @database = database if database.instance_of?(Database)
  end

  def created_at
    @created_at && !@created_at.empty? ? Time.parse(@created_at) : nil
  end

  def updated_at
    @updated_at && !@updated_at.empty? ? Time.parse(@updated_at) : nil
  end

  def last_import
    @last_import && !@last_import.empty? ? Time.parse(@last_import) : nil
  end

  def last_log_timestamp
    @last_log_timestamp && !@last_log_timestamp.empty? ? Time.parse(@last_log_timestamp) : nil
  end

  def expire_days
    @expire_days ? @expire_days.to_i : nil
  end

  def database
    update_database! unless @database
    @database
  end

  # get the database's permission as if they were the table's
  def permission
    database.permission
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

  def update_database!
    @database = @client.database(@db_name)
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

  def initialize(client, job_id, type, query, status=nil, url=nil, debug=nil, start_at=nil, end_at=nil, cpu_time=nil,
                 result_size=nil, result=nil, result_url=nil, hive_result_schema=nil, priority=nil, retry_limit=nil,
                 org_name=nil, db_name=nil)
    super(client)
    @job_id = job_id
    @type = type
    @url = url
    @query = query
    @status = status
    @debug = debug
    @start_at = start_at
    @end_at = end_at
    @cpu_time = cpu_time
    @result_size = result_size
    @result = result
    @result_url = result_url
    @hive_result_schema = hive_result_schema
    @priority = priority
    @retry_limit = retry_limit
    @db_name = db_name
  end

  attr_reader :job_id, :type, :result_url
  attr_reader :priority, :retry_limit, :org_name, :db_name

  def wait(timeout=nil)
    # TODO
  end

  def kill!
    # TODO
  end

  def query
    update_status! unless @query || finished?
    @query
  end

  def status
    update_status! unless @status || finished?
    @status
  end

  def url
    update_status! unless @url || finished?
    @url
  end

  def debug
    update_status! unless @debug || finished?
    @debug
  end

  def start_at
    update_status! unless @start_at || finished?
    @start_at && !@start_at.empty? ? Time.parse(@start_at) : nil
  end

  def end_at
    update_status! unless @end_at || finished?
    @end_at && !@end_at.empty? ? Time.parse(@end_at) : nil
  end

  def cpu_time
    update_status! unless @cpu_time || finished?
    @cpu_time
  end

  def hive_result_schema
    update_status! unless @hive_result_schema.instance_of? Array || finished?
    @hive_result_schema
  end

  def result_size
    update_status! unless @result_size || finished?
    @result_size
  end

  def result
    unless @result
      return nil unless finished?
      @result = @client.job_result(@job_id)
    end
    @result
  end

  def result_format(format, io=nil, &block)
    return nil unless finished?
    @client.job_result_format(@job_id, format, io, &block)
  end

  def result_each_with_compr_size(&block)
    if @result
      @result.each(&block)
    else
      @client.job_result_each_with_compr_size(@job_id, &block)
    end
    nil
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
    update_progress! unless @status
    FINISHED_STATUS.include?(@status)
  end

  def success?
    update_progress! unless @status
    @status == STATUS_SUCCESS
  end

  def error?
    update_progress! unless @status
    @status == STATUS_ERROR
  end

  def killed?
    update_progress! unless @status
    @status == STATUS_KILLED
  end

  def queued?
    update_progress! unless @status
    @status == STATUS_QUEUED
  end

  def running?
    update_progress! unless @status
    @status == STATUS_RUNNING
  end

  def update_progress!
    @status = @client.job_status(@job_id)
  end

  def update_status!
    type, query, status, url, debug, start_at, end_at, cpu_time,
      result_size, result_url, hive_result_schema, priority, retry_limit,
      org_name, db_name = @client.api.show_job(@job_id)
    @query = query
    @status = status
    @url = url
    @debug = debug
    @start_at = start_at
    @end_at = end_at
    @cpu_time = cpu_time
    @result_size = result_size
    @result_url = result_url
    @hive_result_schema = hive_result_schema
    @priority = priority
    @retry_limit = retry_limit
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
  def initialize(client, name, cron, query, database=nil, result_url=nil, timezone=nil, delay=nil, next_time=nil,
                 priority=nil, retry_limit=nil, org_name=nil)
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
    @retry_limit = retry_limit
  end

  attr_reader :name, :cron, :query, :database, :result_url, :timezone, :delay, :priority, :retry_limit, :org_name

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


class User < Model
  def initialize(client, name, org_name, role_names, email)
    super(client)
    @name = name
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


end

