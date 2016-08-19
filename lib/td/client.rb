module TreasureData

require 'td/client/api'
require 'td/client/model'


class Client
  # @param [String] user TreasureData username
  # @param [String] password TreasureData password
  # @param [Hash] opts options for API
  # @return [Client] instance of this class
  def self.authenticate(user, password, opts={})
    api = API.new(nil, opts)
    apikey = api.authenticate(user, password)
    new(apikey)
  end

  # @param [Hash] opts options for API
  # @return [String] HTTP status code of server returns
  def self.server_status(opts={})
    api = API.new(nil, opts)
    api.server_status
  end

  # @param [String] apikey TreasureData API key
  # @param [Hash] opts options for API
  def initialize(apikey, opts={})
    @api = API.new(apikey, opts)
  end

  # @!attribute [r] api
  attr_reader :api

  # @return [String] API key
  def apikey
    @api.apikey
  end

  # @return [String] HTTP status code of server returns
  def server_status
    @api.server_status
  end

  # @param [String] db_name
  # @param [Hash] opts
  # @return [true]
  def create_database(db_name, opts={})
    @api.create_database(db_name, opts)
  end

  # @param [String] db_name
  # @return [Symbol]
  def delete_database(db_name)
    @api.delete_database(db_name)
  end

  # @return [Account]
  def account
    account_id, plan, storage, guaranteed_cores, maximum_cores, created_at = @api.show_account
    return Account.new(self, account_id, plan, storage, guaranteed_cores, maximum_cores, created_at)
  end

  # @param [Fixnum] from
  # @param [Fixnum] to
  # @return [Array] from, to, interval, history
  def core_utilization(from, to)
    from, to, interval, history = @api.account_core_utilization(from, to)
    return from, to, interval, history
  end

  # @return [Array] databases
  def databases
    m = @api.list_databases
    m.map {|db_name,(count, created_at, updated_at, org, permission)|
      Database.new(self, db_name, nil, count, created_at, updated_at, org, permission)
    }
  end

  # @param [String] db_name
  # @return [Database]
  def database(db_name)
    m = @api.list_databases
    m.each {|name,(count, created_at, updated_at, org, permission)|
      if name == db_name
        return Database.new(self, name, nil, count, created_at, updated_at, org, permission)
      end
    }
    raise NotFoundError, "Database '#{db_name}' does not exist"
  end

  # @return [true]
  def create_log_table(db_name, table_name)
    @api.create_log_table(db_name, table_name)
  end

  # Swap table names
  #
  # @param [String] db_name
  # @param [String] table_name1
  # @param [String] table_name2
  # @return [true]
  def swap_table(db_name, table_name1, table_name2)
    @api.swap_table(db_name, table_name1, table_name2)
  end

  # @param [String] db_name
  # @param [String] table_name
  # @param [String] schema
  # @return [true]
  def update_schema(db_name, table_name, schema)
    @api.update_schema(db_name, table_name, schema.to_json)
  end

  # @param [String] db_name
  # @param [String] table_name
  # @param [Fixnum] expire_days
  # @return [true]
  def update_expire(db_name, table_name, expire_days)
    @api.update_expire(db_name, table_name, expire_days)
  end

  # @param [String] db_name
  # @param [String] table_name
  # @return [Symbol]
  def delete_table(db_name, table_name)
    @api.delete_table(db_name, table_name)
  end

  # @param [String] db_name
  # @return [Array] Tables
  def tables(db_name)
    m = @api.list_tables(db_name)
    m.map {|table_name, (type, schema, count, created_at, updated_at, estimated_storage_size, last_import, last_log_timestamp, expire_days)|
      schema = Schema.new.from_json(schema)
      Table.new(self, db_name, table_name, type, schema, count, created_at, updated_at,
        estimated_storage_size, last_import, last_log_timestamp, expire_days)
    }
  end

  # @param [String] db_name
  # @param [String] table_name
  # @return [Table]
  def table(db_name, table_name)
    tables(db_name).each {|t|
      if t.name == table_name
        return t
      end
    }
    raise NotFoundError, "Table '#{db_name}.#{table_name}' does not exist"
  end

  # @param [String] db_name
  # @param [String] table_name
  # @param [Fixnum] count
  # @param [Proc] block
  # @return [Array, nil]
  def tail(db_name, table_name, count, to = nil, from = nil, &block)
    @api.tail(db_name, table_name, count, to, from, &block)
  end

  # @param [String] db_name
  # @param [String] q
  # @param [String] result_url
  # @param [Fixnum] priority
  # @param [Fixnum] retry_limit
  # @param [Hash] opts
  # @return [Job]
  def query(db_name, q, result_url=nil, priority=nil, retry_limit=nil, opts={})
    # for compatibility, assume type is hive unless specifically specified
    type = opts[:type] || opts['type'] || :hive
    raise ArgumentError, "The specified query type is not supported: #{type}" unless [:hive, :pig, :impala, :presto].include?(type)
    job_id = @api.query(q, type, db_name, result_url, priority, retry_limit, opts)
    Job.new(self, job_id, type, q)
  end

  # @param [Fixnum] from
  # @param [Fixnum] to
  # @param [String] status
  # @param [Hash] conditions
  # @return [Job]
  def jobs(from=nil, to=nil, status=nil, conditions=nil)
    results = @api.list_jobs(from, to, status, conditions)
    results.map {|job_id, type, status, query, start_at, end_at, cpu_time,
                 result_size, result_url, priority, retry_limit, org, db,
                 duration|
      Job.new(self, job_id, type, query, status, nil, nil, start_at, end_at, cpu_time,
              result_size, nil, result_url, nil, priority, retry_limit, org, db,
              duration)
    }
  end

  # @param [String] job_id
  # @return [Job]
  def job(job_id)
    job_id = job_id.to_s
    type, query, status, url, debug, start_at, end_at, cpu_time,
      result_size, result_url, hive_result_schema, priority, retry_limit, org, db = @api.show_job(job_id)
    Job.new(self, job_id, type, query, status, url, debug, start_at, end_at, cpu_time,
            result_size, nil, result_url, hive_result_schema, priority, retry_limit, org, db)
  end

  # @param [String] job_id
  # @return [String] HTTP status code
  def job_status(job_id)
    return @api.job_status(job_id)
  end

  # @param [String] job_id
  # @return [Object]
  def job_result(job_id)
    @api.job_result(job_id)
  end

  # @param [String] job_id
  # @param [String] format
  # @param [IO] io
  # @param [Proc] block
  # @return [String]
  def job_result_format(job_id, format, io=nil, &block)
    @api.job_result_format(job_id, format, io, &block)
  end

  def job_result_raw(job_id, format, io=nil, &block)
    @api.job_result_raw(job_id, format, io, &block)
  end

  # @param [String] job_id
  # @param [Proc] block
  # @return [nil]
  def job_result_each(job_id, &block)
    @api.job_result_each(job_id, &block)
  end

  # @param [String] job_id
  # @param [Proc] block
  # @return [nil]
  def job_result_each_with_compr_size(job_id, &block)
    @api.job_result_each_with_compr_size(job_id, &block)
  end

  # @param [String] job_id
  # @return [String] former_status
  def kill(job_id)
    @api.kill(job_id)
  end

  # @param [String] db_name
  # @param [String] table_name
  # @param [String] storage_type
  # @param [Hash] opts
  # @return [Job]
  def export(db_name, table_name, storage_type, opts={})
    job_id = @api.export(db_name, table_name, storage_type, opts)
    Job.new(self, job_id, :export, nil)
  end

  # @param [String] db_name
  # @param [String] table_name
  # @param [Fixnum] to
  # @param [Fixnum] from
  # @param [Hash] opts
  # @return [Job]
  def partial_delete(db_name, table_name, to, from, opts={})
    job_id = @api.partial_delete(db_name, table_name, to, from, opts)
    Job.new(self, job_id, :partialdelete, nil)
  end

  # @param [String] name
  # @param [String] database
  # @param [String] table
  # @param [Hash] opts
  # @return [nil]
  def create_bulk_import(name, database, table, opts={})
    @api.create_bulk_import(name, database, table, opts)
  end

  # @param [String] name
  # @return [nil]
  def delete_bulk_import(name)
    @api.delete_bulk_import(name)
  end

  # @param [String] name
  # @return [nil]
  def freeze_bulk_import(name)
    @api.freeze_bulk_import(name)
  end

  # @param [String] name
  # @return [nil]
  def unfreeze_bulk_import(name)
    @api.unfreeze_bulk_import(name)
  end

  # @param [String] name
  # @param [Hash] opts options for API
  # @return [Job]
  def perform_bulk_import(name, opts={})
    job_id = @api.perform_bulk_import(name, opts)
    Job.new(self, job_id, :bulk_import, nil)
  end

  # @param [String] name
  # @return [nil]
  def commit_bulk_import(name)
    @api.commit_bulk_import(name)
  end

  # @param [String] name
  # @param [Proc] block
  # @return [Hash]
  def bulk_import_error_records(name, &block)
    @api.bulk_import_error_records(name, &block)
  end

  # @param [String] name
  # @return [BulkImport]
  def bulk_import(name)
    data = @api.show_bulk_import(name)
    BulkImport.new(self, data)
  end

  # @return [Array<BulkImport>]
  def bulk_imports
    @api.list_bulk_imports.map {|data|
      BulkImport.new(self, data)
    }
  end

  # @param [String] name
  # @param [String] part_name
  # @param [String, StringIO] stream
  # @param [Fixnum] size
  # @return [nil]
  def bulk_import_upload_part(name, part_name, stream, size)
    @api.bulk_import_upload_part(name, part_name, stream, size)
  end

  # @param [String] name
  # @param [String] part_name
  # @return [nil]
  def bulk_import_delete_part(name, part_name)
    @api.bulk_import_delete_part(name, part_name)
  end

  # @param [String] name
  # @return [Array]
  def list_bulk_import_parts(name)
    @api.list_bulk_import_parts(name)
  end

  # @param [String] name
  # @param [Hash] opts
  # @return [Time]
  def create_schedule(name, opts)
    raise ArgumentError, "'cron' option is required" unless opts[:cron] || opts['cron']
    raise ArgumentError, "'query' option is required" unless opts[:query] || opts['query']
    start = @api.create_schedule(name, opts)
    return start && Time.parse(start)
  end

  # @param [String] name
  # @return [Array]
  def delete_schedule(name)
    @api.delete_schedule(name)
  end

  # @return [Array<Schedule>]
  def schedules
    result = @api.list_schedules
    result.map {|name,cron,query,database,result_url,timezone,delay,next_time,priority,retry_limit,org_name|
      Schedule.new(self, name, cron, query, database, result_url, timezone, delay, next_time, priority, retry_limit, org_name)
    }
  end

  # @param [String] name
  # @param [Hash] params
  # @return [nil]
  def update_schedule(name, params)
    @api.update_schedule(name, params)
    nil
  end

  # @param [String] name
  # @param [Fixnum] from
  # @param [Fixnum] to
  # @return [Array<ScheduledJob>]
  def history(name, from=nil, to=nil)
    result = @api.history(name, from, to)
    result.map {|scheduled_at,job_id,type,status,query,start_at,end_at,result_url,priority,database|
      job_param = [job_id, type, query, status,
        nil, nil, # url, debug
        start_at, end_at,
        nil, # cpu_time
        nil, nil, # result_size, result
        result_url,
        nil, # hive_result_schema
        priority,
        nil, # retry_limit
        nil, # TODO org_name
        database]
      ScheduledJob.new(self, scheduled_at, *job_param)
    }
  end

  # @param [String] name
  # @param [Fixnum] time UNIX timestamp
  # @param [Fixnum] num
  # @return [Array<ScheduledJob>]
  def run_schedule(name, time, num)
    results = @api.run_schedule(name, time, num)
    results.map {|job_id,type,scheduled_at|
      ScheduledJob.new(self, scheduled_at, job_id, type, nil)
    }
  end

  # @param [String] db_name
  # @param [String] table_name
  # @param [String] format
  # @param [String, StringIO] stream
  # @param [Fixnum] size
  # @param [String] unique_id
  # @return [Float]
  def import(db_name, table_name, format, stream, size, unique_id=nil)
    @api.import(db_name, table_name, format, stream, size, unique_id)
  end

  # @return [Array<Result>]
  def results
    results = @api.list_result
    rs = results.map {|name,url,organizations|
      Result.new(self, name, url, organizations)
    }
    return rs
  end

  # @param [String] name
  # @param [String] url
  # @param [Hash] opts
  # @return [true]
  def create_result(name, url, opts={})
    @api.create_result(name, url, opts)
  end

  # @param [String] name
  # @return [true]
  def delete_result(name)
    @api.delete_result(name)
  end

  # @return [Array<User>]
  def users
    list = @api.list_users
    list.map {|name,org,roles,email|
      User.new(self, name, org, roles, email)
    }
  end

  # @param [String] name
  # @param [String] org
  # @param [String] email
  # @param [String] password
  # @return [true]
  def add_user(name, org, email, password)
    @api.add_user(name, org, email, password)
  end

  # @param [String] user
  # @return [true]
  def remove_user(user)
    @api.remove_user(user)
  end

  # @param [String] user
  # @param [String] email
  # @return [true]
  def change_email(user, email)
    @api.change_email(user, email)
  end

  # @param [String] user
  # @return [Array<String>]
  def list_apikeys(user)
    @api.list_apikeys(user)
  end

  # @param [String] user
  # @return [true]
  def add_apikey(user)
    @api.add_apikey(user)
  end

  # @param [String] user
  # @param [String] apikey
  # @return [true]
  def remove_apikey(user, apikey)
    @api.remove_apikey(user, apikey)
  end

  # @param [String] user
  # @param [String] password
  # @return [true]
  def change_password(user, password)
    @api.change_password(user, password)
  end

  # @param [String] old_password
  # @param [String] password
  # @return [true]
  def change_my_password(old_password, password)
    @api.change_my_password(old_password, password)
  end

  # @return [Array<AccessControl>]
  def access_controls
    list = @api.list_access_controls
    list.map {|subject,action,scope,grant_option|
      AccessControl.new(self, subject, action, scope, grant_option)
    }
  end

  # @param [String] subject
  # @param [String] action
  # @param [String] scope
  # @param [Array] grant_option
  # @return [true]
  def grant_access_control(subject, action, scope, grant_option)
    @api.grant_access_control(subject, action, scope, grant_option)
  end

  # @param [String] subject
  # @param [String] action
  # @param [String] scope
  # @return [true]
  def revoke_access_control(subject, action, scope)
    @api.revoke_access_control(subject, action, scope)
  end

  # @param [String] user
  # @param [String] action
  # @param [String] scope
  # @return [Array]
  def test_access_control(user, action, scope)
    @api.test_access_control(user, action, scope)
  end

  # => BulkLoad::Job
  def bulk_load_guess(job)
    @api.bulk_load_guess(job)
  end

  # => BulkLoad::Job
  def bulk_load_preview(job)
    @api.bulk_load_preview(job)
  end

  # => String
  def bulk_load_issue(database, table, job)
    @api.bulk_load_issue(database, table, job)
  end

  # nil -> [BulkLoad]
  def bulk_load_list
    @api.bulk_load_list
  end

  # name: String, database: String, table: String, job: BulkLoad -> BulkLoad
  def bulk_load_create(name, database, table, job, opts = {})
    @api.bulk_load_create(name, database, table, job, opts)
  end

  # name: String -> BulkLoad
  def bulk_load_show(name)
    @api.bulk_load_show(name)
  end

  # name: String, settings: Hash -> BulkLoad
  def bulk_load_update(name, settings)
    @api.bulk_load_update(name, settings)
  end

  # name: String -> BulkLoad
  def bulk_load_delete(name)
    @api.bulk_load_delete(name)
  end

  # name: String -> [Job]
  def bulk_load_history(name)
    @api.bulk_load_history(name)
  end

  def bulk_load_run(name, scheduled_time = nil)
    @api.bulk_load_run(name, scheduled_time)
  end

end

end # module TreasureData
