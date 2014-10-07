module TreasureData

require 'td/client/api'
require 'td/client/model'


class Client
  def self.authenticate(user, password, opts={})
    api = API.new(nil, opts)
    apikey = api.authenticate(user, password)
    new(apikey)
  end

  def self.server_status(opts={})
    api = API.new(nil, opts)
    api.server_status
  end

  def initialize(apikey, opts={})
    @api = API.new(apikey, opts)
  end

  attr_reader :api

  def apikey
    @api.apikey
  end

  def server_status
    @api.server_status
  end

  # => true
  def create_database(db_name, opts={})
    @api.create_database(db_name, opts)
  end

  # => true
  def delete_database(db_name)
    @api.delete_database(db_name)
  end

  # => Account
  def account
    account_id, plan, storage, guaranteed_cores, maximum_cores, created_at = @api.show_account
    return Account.new(self, account_id, plan, storage, guaranteed_cores, maximum_cores, created_at)
  end

  def core_utilization(from, to)
    from, to, interval, history = @api.account_core_utilization(from, to)
    return from, to, interval, history
  end

  # => [Database]
  def databases
    m = @api.list_databases
    m.map {|db_name,(count, created_at, updated_at, org, permission)|
      Database.new(self, db_name, nil, count, created_at, updated_at, org, permission)
    }
  end

  # => Database
  def database(db_name)
    m = @api.list_databases
    m.each {|name,(count, created_at, updated_at, org, permission)|
      if name == db_name
        return Database.new(self, name, nil, count, created_at, updated_at, org, permission)
      end
    }
    raise NotFoundError, "Database '#{db_name}' does not exist"
  end

  # => true
  def create_log_table(db_name, table_name)
    @api.create_log_table(db_name, table_name)
  end

  # => true
  def create_item_table(db_name, table_name, primary_key, primary_key_type)
    @api.create_item_table(db_name, table_name, primary_key, primary_key_type)
  end

  # => true
  def swap_table(db_name, table_name1, table_name2)
    @api.swap_table(db_name, table_name1, table_name2)
  end

  # => true
  def update_schema(db_name, table_name, schema)
    @api.update_schema(db_name, table_name, schema.to_json)
  end

  # => true
  def update_expire(db_name, table_name, expire_days)
    @api.update_expire(db_name, table_name, expire_days)
  end

  # => type:Symbol
  def delete_table(db_name, table_name)
    @api.delete_table(db_name, table_name)
  end

  # => [Table]
  def tables(db_name)
    m = @api.list_tables(db_name)
    m.map {|table_name, (type, schema, count, created_at, updated_at, estimated_storage_size, last_import, last_log_timestamp, expire_days, primary_key, primary_key_type)|
      schema = Schema.new.from_json(schema)
      Table.new(self, db_name, table_name, type, schema, count, created_at, updated_at,
        estimated_storage_size, last_import, last_log_timestamp, expire_days, primary_key, primary_key_type)
    }
  end

  # => Table
  def table(db_name, table_name)
    tables(db_name).each {|t|
      if t.name == table_name
        return t
      end
    }
    raise NotFoundError, "Table '#{db_name}.#{table_name}' does not exist"
  end

  def tail(db_name, table_name, count, to=nil, from=nil, &block)
    @api.tail(db_name, table_name, count, to, from, &block)
  end

  # => Job
  def query(db_name, q, result_url=nil, priority=nil, retry_limit=nil, opts={})
    # for compatibility, assume type is hive unless specifically specified
    type = opts[:type] || opts['type'] || :hive
    raise ArgumentError, "The specified query type is not supported: #{type}" unless [:hive, :pig, :impala, :presto].include?(type)
    job_id = @api.query(q, type, db_name, result_url, priority, retry_limit, opts)
    Job.new(self, job_id, type, q)
  end

  # => [Job]
  def jobs(from=nil, to=nil, status=nil, conditions=nil)
    results = @api.list_jobs(from, to, status, conditions)
    results.map {|job_id, type, status, query, start_at, end_at, cpu_time,
                 result_size, result_url, priority, retry_limit, org, db|
      Job.new(self, job_id, type, query, status, nil, nil, start_at, end_at, cpu_time,
              result_size, nil, result_url, nil, priority, retry_limit, org, db)
    }
  end

  # => Job
  def job(job_id)
    job_id = job_id.to_s
    type, query, status, url, debug, start_at, end_at, cpu_time,
      result_size, result_url, hive_result_schema, priority, retry_limit, org, db = @api.show_job(job_id)
    Job.new(self, job_id, type, query, status, url, debug, start_at, end_at, cpu_time,
            result_size, nil, result_url, hive_result_schema, priority, retry_limit, org, db)
  end

  # => status:String
  def job_status(job_id)
    return @api.job_status(job_id)
  end

  # => result:[{column:String=>value:Object]
  def job_result(job_id)
    @api.job_result(job_id)
  end

  # => result:String
  def job_result_format(job_id, format, io=nil, &block)
    @api.job_result_format(job_id, format, io, &block)
  end

  # => nil
  def job_result_each(job_id, &block)
    @api.job_result_each(job_id, &block)
  end

  # => nil
  def job_result_each_with_compr_size(job_id, &block)
    @api.job_result_each_with_compr_size(job_id, &block)
  end

  # => former_status:String
  def kill(job_id)
    @api.kill(job_id)
  end

  # => Job
  def export(db_name, table_name, storage_type, opts={})
    job_id = @api.export(db_name, table_name, storage_type, opts)
    Job.new(self, job_id, :export, nil)
  end

  # => Job
  def partial_delete(db_name, table_name, to, from, opts={})
    job_id = @api.partial_delete(db_name, table_name, to, from, opts)
    Job.new(self, job_id, :partialdelete, nil)
  end

  # => nil
  def create_bulk_import(name, database, table, opts={})
    @api.create_bulk_import(name, database, table, opts)
  end

  # => nil
  def delete_bulk_import(name)
    @api.delete_bulk_import(name)
  end

  # => nil
  def freeze_bulk_import(name)
    @api.freeze_bulk_import(name)
  end

  # => nil
  def unfreeze_bulk_import(name)
    @api.unfreeze_bulk_import(name)
  end

  # => Job
  def perform_bulk_import(name)
    job_id = @api.perform_bulk_import(name)
    Job.new(self, job_id, :bulk_import, nil)
  end

  # => nil
  def commit_bulk_import(name)
    @api.commit_bulk_import(name)
  end

  # => records:[row:Hash]
  def bulk_import_error_records(name, &block)
    @api.bulk_import_error_records(name, &block)
  end

  # => BulkImport
  def bulk_import(name)
    data = @api.show_bulk_import(name)
    BulkImport.new(self, data)
  end

  # => [BulkImport]
  def bulk_imports
    @api.list_bulk_imports.map {|data|
      BulkImport.new(self, data)
    }
  end

  # => nil
  def bulk_import_upload_part(name, part_name, stream, size)
    @api.bulk_import_upload_part(name, part_name, stream, size)
  end

  # => nil
  def bulk_import_delete_part(name, part_name)
    @api.bulk_import_delete_part(name, part_name)
  end

  def list_bulk_import_parts(name)
    @api.list_bulk_import_parts(name)
  end

  # => first_time:Time
  def create_schedule(name, opts)
    raise ArgumentError, "'cron' option is required" unless opts[:cron] || opts['cron']
    raise ArgumentError, "'query' option is required" unless opts[:query] || opts['query']
    start = @api.create_schedule(name, opts)
    return Time.parse(start)
  end

  # => true
  def delete_schedule(name)
    @api.delete_schedule(name)
  end

  # [Schedule]
  def schedules
    result = @api.list_schedules
    result.map {|name,cron,query,database,result_url,timezone,delay,next_time,priority,retry_limit,org_name|
      Schedule.new(self, name, cron, query, database, result_url, timezone, delay, next_time, priority, retry_limit, org_name)
    }
  end

  def update_schedule(name, params)
    @api.update_schedule(name, params)
    nil
  end

  # [ScheduledJob]
  def history(name, from=nil, to=nil)
    result = @api.history(name, from, to)
    result.map {|scheduled_at,job_id,type,status,query,start_at,end_at,result_url,priority,database|
      # TODO org
      ScheduledJob.new(self, scheduled_at, job_id, type, query, status, nil, nil, start_at, end_at, nil, result_url, nil, priority, nil, nil, database)
    }
  end

  # [ScheduledJob]
  def run_schedule(name, time, num)
    results = @api.run_schedule(name, time, num)
    results.map {|job_id,type,scheduled_at|
      ScheduledJob.new(self, scheduled_at, job_id, type, nil)
    }
  end

  # => time:Flaot
  def import(db_name, table_name, format, stream, size, unique_id=nil)
    @api.import(db_name, table_name, format, stream, size, unique_id)
  end

  # => [Result]
  def results
    results = @api.list_result
    rs = results.map {|name,url,organizations|
      Result.new(self, name, url, organizations)
    }
    return rs
  end

  # => true
  def create_result(name, url, opts={})
    @api.create_result(name, url, opts)
  end

  # => true
  def delete_result(name)
    @api.delete_result(name)
  end

  # => [User]
  def users
    list = @api.list_users
    list.map {|name,org,roles,email|
      User.new(self, name, org, roles, email)
    }
  end

  # => true
  def add_user(name, org, email, password)
    @api.add_user(name, org, email, password)
  end

  # => true
  def remove_user(user)
    @api.remove_user(user)
  end

  # => true
  def change_email(user, email)
    @api.change_email(user, email)
  end

  # => [apikey:String]
  def list_apikeys(user)
    @api.list_apikeys(user)
  end

  # => true
  def add_apikey(user)
    @api.add_apikey(user)
  end

  # => true
  def remove_apikey(user, apikey)
    @api.remove_apikey(user, apikey)
  end

  # => true
  def change_password(user, password)
    @api.change_password(user, password)
  end

  # => true
  def change_my_password(old_password, password)
    @api.change_my_password(old_password, password)
  end

  # => [User]
  def access_controls
    list = @api.list_access_controls
    list.map {|subject,action,scope,grant_option|
      AccessControl.new(self, subject, action, scope, grant_option)
    }
  end

  # => true
  def grant_access_control(subject, action, scope, grant_option)
    @api.grant_access_control(subject, action, scope, grant_option)
  end

  # => true
  def revoke_access_control(subject, action, scope)
    @api.revoke_access_control(subject, action, scope)
  end

  # => true
  def test_access_control(user, action, scope)
    @api.test_access_control(user, action, scope)
  end
end

end # module TreasureData
