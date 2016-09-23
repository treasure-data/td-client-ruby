require 'timeout'

module TreasureData

class Model
  # @param [TreasureData::Client] client
  def initialize(client)
    @client = client
  end

  # @!attribute [r] client
  # @return [TreasureData::Client] client
  attr_reader :client
end

class Account < Model
  # @param [TreasureData::Client] client
  # @param [String] account_id
  # @param [Fixnum] plan
  # @param [Fixnum] storage_size
  # @param [Fixnum] guaranteed_cores
  # @param [Fixnum] maximum_cores
  # @param [String] created_at
  def initialize(client, account_id, plan, storage_size=nil, guaranteed_cores=nil, maximum_cores=nil, created_at=nil)
    super(client)
    @account_id = account_id
    @plan = plan
    @storage_size = storage_size
    @guaranteed_cores = guaranteed_cores
    @maximum_cores = maximum_cores
    @created_at = created_at
  end

  # @!attribute [r] account_id
  # @!attribute [r] plan
  # @!attribute [r] storage_size
  # @!attribute [r] guaranteed_cores
  # @!attribute [r] maximum_cores
  attr_reader :account_id, :plan, :storage_size, :guaranteed_cores, :maximum_cores

  # @return <Time, nil>
  def created_at
    @created_at && !@created_at.empty? ? Time.parse(@created_at) : nil
  end

  # @return <String>
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

  # @param [TreasureData::Client] client
  # @param [String] db_name
  # @param [Array<Table>] tables
  # @param [Fixnum] count
  # @param [String] created_at
  # @param [String] updated_at
  # @param [String] org_name
  # @param [String] permission
  def initialize(client, db_name, tables=nil, count=nil, created_at=nil, updated_at=nil, org_name=nil, permission=nil)
    super(client)
    @db_name = db_name
    @tables = tables
    @count = count
    @created_at = created_at
    @updated_at = updated_at
    @permission = permission.to_sym
  end

  # @!attribute [r] org_name
  # @!attribute [r] permission
  # @!attribute [r] count
  attr_reader :org_name, :permission, :count

  # @return [String] db_name
  def name
    @db_name
  end

  # @return [Array<Table>]
  def tables
    update_tables! unless @tables
    @tables
  end

  # @param [String] name
  # @return [true]
  def create_log_table(name)
    @client.create_log_table(@db_name, name)
  end

  # @param [String] table_name
  # @return [Table]
  def table(table_name)
    @client.table(@db_name, table_name)
  end

  # @return [Symbol]
  def delete
    @client.delete_database(@db_name)
  end

  # @param [String] q
  # @return [Job]
  def query(q)
    @client.query(@db_name, q)
  end

  # @return [Time, nil]
  def created_at
    @created_at && !@created_at.empty? ? Time.parse(@created_at) : nil
  end

  # @return [Time, nil]
  def updated_at
    @updated_at && !@updated_at.empty? ? Time.parse(@updated_at) : nil
  end

  # @return [nil]
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
  # @param [TreasureData::Client] client
  # @param [String] db_name
  # @param [String] table_name
  # @param [String] type
  # @param [String] schema
  # @param [Fixnum] count
  # @param [String] created_at
  # @param [String] updated_at
  # @param [Fixnum] estimated_storage_size
  # @param [String] last_import
  # @param [String] last_log_timestamp
  # @param [Fixnum, String] expire_days
  def initialize(client, db_name, table_name, type, schema, count, created_at=nil, updated_at=nil, estimated_storage_size=nil, last_import=nil, last_log_timestamp=nil, expire_days=nil)
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
  end

  # @!attribute [r] type
  # @!attribute [r] db_name
  # @!attribute [r] table_name
  # @!attribute [r] schema
  # @!attribute [r] count
  # @!attribute [r] estimated_storage_size
  attr_reader :type, :db_name, :table_name, :schema, :count, :estimated_storage_size

  alias database_name db_name
  alias name table_name

  # @param [String] database
  def database=(database)
    @database = database if database.instance_of?(Database)
  end

  # @return [Time, nil]
  def created_at
    @created_at && !@created_at.empty? ? Time.parse(@created_at) : nil
  end

  # @return [Time, nil]
  def updated_at
    @updated_at && !@updated_at.empty? ? Time.parse(@updated_at) : nil
  end

  # @return [Time, nil]
  def last_import
    @last_import && !@last_import.empty? ? Time.parse(@last_import) : nil
  end

  # @return [Time, nil]
  def last_log_timestamp
    @last_log_timestamp && !@last_log_timestamp.empty? ? Time.parse(@last_log_timestamp) : nil
  end

  # @return [Fixnum, nil]
  def expire_days
    @expire_days ? @expire_days.to_i : nil
  end

  # @return [Database]
  def database
    update_database! unless @database
    @database
  end

  # get the database's permission as if they were the table's
  # @return [String]
  def permission
    database.permission
  end

  # @return [String]
  def identifier
    "#{@db_name}.#{@table_name}"
  end

  # @return [Symbol]
  def delete
    @client.delete_table(@db_name, @table_name)
  end

  # @param [Fixnum] count
  # @return [Array, nil]
  def tail(count)
    @client.tail(@db_name, @table_name, count)
  end

  # @param [String] format
  # @param [String, StringIO] stream
  # @param [Fixnum] size
  # @return [Float]
  def import(format, stream, size)
    @client.import(@db_name, @table_name, format, stream, size)
  end

  # @param [String] storage_type
  # @param [Hash] opts
  # @return [Job]
  def export(storage_type, opts={})
    @client.export(@db_name, @table_name, storage_type, opts)
  end

  # @return [String]
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

  # @return [String]
  def inspect
    %[#<%s:%#0#{1.size*2}x @db_name="%s" @table_name="%s">] %
    [self.class.name, self.__id__*2, @db_name, @table_name]
  end
end

class Schema
  class Field
    # @param [String] name
    # @param [String] type
    # @param [String] sql_alias
    def initialize(name, type, sql_alias=nil)
      if name == 'v' || name == 'time'
        raise ParameterValidationError, "Column name '#{name}' is reserved."
      end
      API.validate_column_name(name)
      API.validate_sql_alias_name(sql_alias) if sql_alias
      @name = name
      @type = type
      @sql_alias = sql_alias
    end

    # @!attribute [r] name
    # @!attribute [r] type
    attr_reader :name
    attr_reader :type
    attr_reader :sql_alias
  end

  # @param [String] columns
  # @return [Schema]
  def self.parse(columns)
    schema = Schema.new
    columns.each {|column|
      unless /\A(?<name>.*)(?::(?<type>[^:]+))(?:@(?<sql_alias>[^:@]+))?\z/ =~ column
        raise ParameterValidationError, "type must be specified"
      end
      schema.add_field(name, type, sql_alias)
    }
    schema
  end

  # @param [Array] fields
  def initialize(fields=[])
    @fields = fields
    @names = {}
    @fields.each do |f|
      raise ArgumentError, "Column name '#{f.name}' is duplicated." if @names.key?(f.name)
      @names[f.name] = true
      if f.sql_alias
        raise ArgumentError, "SQL Column alias '#{f.sql_alias}' is duplicated." if @names.key?(f.sql_alias)
        @names[f.sql_alias] = true
      end
    end
  end

  # @!attribute [r] fields
  attr_reader :fields

  # @param [String] name
  # @param [String] type
  # @return [Array]
  def add_field(name, type, sql_alias=nil)
    if @names.key?(name)
      raise ParameterValidationError, "Column name '#{name}' is duplicated."
    end
    @names[name] = true
    if sql_alias && @names.key?(sql_alias)
      raise ParameterValidationError, "SQL Column alias '#{sql_alias}' is duplicated."
    end
    @names[sql_alias] = true
    @fields << Field.new(name, type, sql_alias)
  end

  # @param [Schema] schema
  # @return [Schema]
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

  # @return [Array<Field>]
  def to_json(*args)
    @fields.map {|f| f.sql_alias ? [f.name, f.type, f.sql_alias] : [f.name, f.type] }.to_json(*args)
  end

  # @param [Object] obj
  # @return [self]
  def from_json(obj)
    @fields = obj.map {|f|
      Field.new(*f)
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

  # @param [TreasureData::Client] client
  # @param [String] job_id
  # @param [String] type
  # @param [String] query
  # @param [Fixnum] status
  # @param [String] url
  # @param [Boolean] debug
  # @param [String] start_at
  # @param [String] end_at
  # @param [String] cpu_time
  # @param [String] result_size
  # @param [Array] result
  # @param [String] result_url
  # @param [Array] hive_result_schema
  # @param [Fixnum] priority
  # @param [Fixnum] retry_limit
  # @param [String] org_name
  # @param [String] db_name
  # @param [Fixnum] duration
  def initialize(client, job_id, type, query, status=nil, url=nil, debug=nil, start_at=nil, end_at=nil, cpu_time=nil,
                 result_size=nil, result=nil, result_url=nil, hive_result_schema=nil, priority=nil, retry_limit=nil,
                 org_name=nil, db_name=nil, duration=nil)
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
    @duration = duration
  end

  # @!attribute [r] job_id
  # @!attribute [r] type
  # @!attribute [r] result_url
  # @!attribute [r] priority
  # @!attribute [r] retry_limit
  # @!attribute [r] org_name
  # @!attribute [r] db_name
  # @!attribute [r] duration
  attr_reader :job_id, :type, :result_url
  attr_reader :priority, :retry_limit, :org_name, :db_name
  attr_reader :duration

  def wait(timeout=nil, wait_interval=2)
    # this should use monotonic clock but td-client-ruby supports Ruby 1.8.7 now.
    # therefore add workaround to initializing clock on each delta
    if timeout
      orig_timeout = timeout
      t1 = Time.now.to_f
    end
    until finished?
      if timeout
        t = Time.now.to_f
        d = t - t1
        t1 = t
        timeout -= d if d > 0
        raise Timeout::Error, "timeout=#{orig_timeout} wait_interval=#{wait_interval}" if timeout <= 0
      end
      sleep wait_interval
      yield self if block_given?
      update_progress!
    end
  end

  def kill!
    # TODO
  end

  # @return [String]
  def query
    update_status! unless @query || finished?
    @query
  end

  # @return [String]
  def status
    update_status! unless @status || finished?
    @status
  end

  # @return [String]
  def url
    update_status! unless @url || finished?
    @url
  end

  # @return [Boolean]
  def debug
    update_status! unless @debug || finished?
    @debug
  end

  # @return [Time, nil]
  def start_at
    update_status! unless @start_at || finished?
    @start_at && !@start_at.empty? ? Time.parse(@start_at) : nil
  end

  # @return [Time, nil]
  def end_at
    update_status! unless @end_at || finished?
    @end_at && !@end_at.empty? ? Time.parse(@end_at) : nil
  end

  # @return [String]
  def cpu_time
    update_status! unless @cpu_time || finished?
    @cpu_time
  end

  # @return [Array]
  def hive_result_schema
    update_status! unless @hive_result_schema.instance_of? Array || finished?
    @hive_result_schema
  end

  # @return [String]
  def result_size
    update_status! unless @result_size || finished?
    @result_size
  end

  # @return [Array]
  def result
    unless @result
      return nil unless finished?
      @result = @client.job_result(@job_id)
    end
    @result
  end

  # @param [String] format
  # @param [IO] io
  # @param [Proc] block
  # @return [nil, String]
  def result_format(format, io=nil, &block)
    return nil unless finished?
    @client.job_result_format(@job_id, format, io, &block)
  end

  def result_raw(format, io=nil, &block)
    return nil unless finished?
    @client.job_result_raw(@job_id, format, io, &block)
  end

  # @yield [result]
  # @return [nil]
  def result_each_with_compr_size(&block)
    if @result
      @result.each(&block)
    else
      @client.job_result_each_with_compr_size(@job_id, &block)
    end
    nil
  end

  # @yield [result]
  # @return [nil]
  def result_each(&block)
    if @result
      @result.each(&block)
    else
      @client.job_result_each(@job_id, &block)
    end
    nil
  end

  # @return [Boolean]
  def finished?
    update_progress! unless @status
    FINISHED_STATUS.include?(@status)
  end

  # @return [Boolean]
  def success?
    update_progress! unless @status
    @status == STATUS_SUCCESS
  end

  # @return [Boolean]
  def error?
    update_progress! unless @status
    @status == STATUS_ERROR
  end

  # @return [Boolean]
  def killed?
    update_progress! unless @status
    @status == STATUS_KILLED
  end

  # @return [Boolean]
  def queued?
    update_progress! unless @status
    @status == STATUS_QUEUED
  end

  # @return [Boolean]
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
  attr_reader :scheduled_at

  # @param [TreasureData::Client] client
  # @param [String] scheduled_at
  # @param [...] super_args for Job#initialize
  def initialize(client, scheduled_at, *super_args)
    super(client, *super_args)
    if scheduled_at.to_s.empty?
      @scheduled_at = nil
    else
      @scheduled_at = Time.parse(scheduled_at) rescue nil
    end
  end
end


class Schedule < Model
  # @param [TreasureData::Client] client
  # @param [String] name
  # @param [String] cron
  # @param [String] query
  # @param [Fixnum] database
  # @param [String] result_url
  # @param [String] timezone
  # @param [String] delay
  # @param [String] next_time
  # @param [String] priority
  # @param [String] retry_limit
  # @param [String] org_name
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

  # @!attribute [r] name
  # @!attribute [r] cron
  # @!attribute [r] query
  # @!attribute [r] database
  # @!attribute [r] result_url
  # @!attribute [r] delay
  # @!attribute [r] priority
  # @!attribute [r] retry_limit
  # @!attribute [r] org_name
  attr_reader :name, :cron, :query, :database, :result_url, :timezone, :delay, :priority, :retry_limit, :org_name

  # @return [Time, nil]
  def next_time
    @next_time ? Time.parse(@next_time) : nil
  end

  # @param [String] time
  # @param [Fixnum] num
  # @return [Array]
  def run(time, num)
    @client.run_schedule(@name, time, num)
  end
end


class Result < Model
  # @param [TreasureData::Client] client
  # @param [String] name
  # @param [String] url
  # @param [String] org_name
  def initialize(client, name, url, org_name)
    super(client)
    @name = name
    @url = url
  end

  # @!attribute [r] name
  # @!attribute [r] url
  # @!attribute [r] org_name
  attr_reader :name, :url, :org_name
end


class BulkImport < Model
  # @param [TreasureData::Client] client
  # @param [Hash] data
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

  # @!attribute [r] name
  # @!attribute [r] database
  # @!attribute [r] table
  # @!attribute [r] status
  # @!attribute [r] job_id
  # @!attribute [r] valid_records
  # @!attribute [r] error_records
  # @!attribute [r] valid_parts
  # @!attribute [r] error_parts
  # @!attribute [r] org_name
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

  # @return [Boolean]
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
  # @param [TreasureData::Client] client
  # @param [String] subject
  # @param [String] action
  # @param [String] scope
  # @param [Array] grant_option
  def initialize(client, subject, action, scope, grant_option)
    super(client)
    @subject = subject
    @action = action
    @scope = scope
    @grant_option = grant_option
  end

  # @!attribute [r] subject
  # @!attribute [r] action
  # @!attribute [r] scope
  # @!attribute [r] grant_option
  attr_reader :subject, :action, :scope, :grant_option
end


end
