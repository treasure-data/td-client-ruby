
module TreasureData


class Model
  def initialize(client)
    @client = client
  end
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

  def initialize(client, job_id, type, query, status=nil, url=nil, debug=nil, start_at=nil, end_at=nil, result=nil)
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
  end

  attr_reader :job_id, :type

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
    query, status, url, debug, start_at, end_at = @client.job_status(@job_id)
    @query = query
    @status = status
    @url = url
    @debug = debug
    @start_at = start_at
    @end_at = end_at
    self
  end
end


end

