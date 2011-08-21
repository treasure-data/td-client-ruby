
module TreasureData

require 'td/client/api'
require 'td/client/model'


class Client
  def self.authenticate(user, password)
    api = API.new(nil)
    apikey = api.authenticate(user, password)
    new(apikey)
  end

  def self.server_status
    api = API.new(nil)
    api.server_status
  end

  def initialize(apikey)
    @api = API.new(apikey)
  end

  attr_reader :api

  def apikey
    @api.apikey
  end

  def server_status
    @api.server_status
  end

  # => true
  def create_database(db_name)
    @api.create_database(db_name)
  end

  # => true
  def delete_database(db_name)
    @api.delete_database(db_name)
  end

  # => [Database]
  def databases
    names = @api.list_databases
    names.map {|db_name|
      Database.new(self, db_name)
    }
  end

  # => Database
  def database(db_name)
    names = @api.list_databases
    names.each {|n|
      if n == db_name
        return Database.new(self, db_name)
      end
    }
    raise NotFoundError, "Database '#{db_name}' does not exist"
  end

  # => true
  def create_log_table(db_name, table_name)
    @api.create_log_table(db_name, table_name)
  end

  # => true
  def create_item_table(db_name, table_name)
    @api.create_item_table(db_name, table_name)
  end

  # => true
  def update_schema(db_name, table_name, schema)
    @api.update_schema(db_name, table_name, schema.to_json)
  end

  # => type:Symbol
  def delete_table(db_name, table_name)
    @api.delete_table(db_name, table_name)
  end

  # => [Table]
  def tables(db_name)
    m = @api.list_tables(db_name)
    m.map {|table_name,(type,schema,count)|
      schema = Schema.new.from_json(schema)
      Table.new(self, db_name, table_name, type, schema, count)
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

  # => Job
  def query(db_name, q)
    job_id = @api.hive_query(q, db_name)
    Job.new(self, job_id, :hive, q)  # TODO url
  end

  # => [Job=]
  def jobs(from=nil, to=nil)
    js = @api.list_jobs(from, to)
    js.map {|job_id,type,status,query,start_at,end_at|
      Job.new(self, job_id, type, query, status, nil, nil, start_at, end_at)
    }
  end

  # => Job
  def job(job_id)
    job_id = job_id.to_s
    type, query, status, url, debug, start_at, end_at = @api.show_job(job_id)
    Job.new(self, job_id, type, query, status, url, debug, start_at, end_at)
  end

  # => type:Symbol, url:String
  def job_status(job_id)
    type, query, status, url, debug, start_at, end_at = @api.show_job(job_id)
    return query, status, url, debug, start_at, end_at
  end

  # => result:[{column:String=>value:Object]
  def job_result(job_id)
    @api.job_result(job_id)
  end

  # => result:String
  def job_result_format(job_id, format)
    @api.job_result_format(job_id, format)
  end

  # => nil
  def job_result_each(job_id, &block)
    @api.job_result_each(job_id, &block)
  end

  # => time:Flaot
  def import(db_name, table_name, format, stream, size)
    @api.import(db_name, table_name, format, stream, size)
  end
end


end

