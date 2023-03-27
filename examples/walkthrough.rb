$LOAD_PATH << File.join(File.dirname(__FILE__), '..', 'lib')

require "msgpack"
require "tempfile"
require "stringio"
require "zlib"
require "td-client"

include TreasureData

class Example
  def initialize(api_key, opts={})
    @client = Client.new(api_key, opts)
  end

  # Utils
  def put_title(title)
    puts title
    puts "-" * 3
  end

  def put_separator
    puts "*" * 50
    puts ""
  end

  def wait_job(job)
    # wait for job to be finished
    cmdout_lines = 0
    stderr_lines = 0
    job.wait(nil, detail: true, verbose: true) do
      cmdout = job.debug['cmdout'].to_s.split("\n")[cmdout_lines..-1] || []
       stderr = job.debug['stderr'].to_s.split("\n")[stderr_lines..-1] || []
       (cmdout + stderr).each {|line|
         $stdout.puts "  "+line
       }
       cmdout_lines += cmdout.size
       stderr_lines += stderr.size
    end
  end
  # API
  def server_status
    begin
      server_status = @client.server_status
      puts "Server Status: #{server_status}"
    rescue StandardError => e
      puts e.message
    end
  end

  def account_info
    put_title "Account"
    begin
      account = @client.account
      puts "ID: #{account.account_id}"
      puts "Plan: #{account.plan}"
      puts "Storage Size: #{account.storage_size}"
      puts "Guaranteed cores: #{account.guaranteed_cores}"
      puts "Maximum cores: #{account.maximum_cores}"
      puts "Created at: #{account.created_at}"
    rescue StandardError => e
      puts e.message
    end
  end

  def list_databases
    put_title "Databases"
    begin
      databases = @client.databases
      for db in databases
        puts "Name: #{db.name}"
        puts "Created at: #{db.created_at}"
      end
    rescue StandardError => e
      puts e.message
    end
  end

  def create_database(name)
    put_title "Create database"
    begin
      flag = @client.create_database(name)
      puts "Created database #{name} successfully!"
    rescue StandardError => e
      puts e.message
    end
  end

  def delete_database(name)
    put_title "Delete database"
    begin
      flag = @client.delete_database(name)
      puts "Deleted database #{name} successfully!"
    rescue StandardError => e
      puts e.message
    end
  end

  def create_log_table(db, name)
    put_title "Create log table"
    begin
      flag = @client.create_log_table(db, name)
      puts "Created log table #{name} in database #{db}!"
    rescue StandardError => e
      puts e.message
    end
  end

  def update_schema(db, table, schema)
    put_title "Update schema"
    begin
      flag = @client.update_schema(db, table, schema)
      puts "Updated schema for table #{table} in database #{db}!"
    rescue StandardError => e
      puts e.message
    end
  end

  def import_data(db, table)
    put_title "Import data"

    sample_data = {
      "time" => "1",
      "col1" => "value1",
      "col2" => "value2"
    }

    out = Tempfile.new("td-import")
    out.binmode if out.respond_to?(:binmode)

    writer = Zlib::GzipWriter.new(out)

    begin
      writer.write sample_data.to_msgpack
      writer.finish

      size = out.pos
      out.pos = 0

      time = @client.import(db, table, "msgpack.gz", out, size)
      puts "Importing data is done in #{time}"
    rescue StandardError => e
      puts e.message
    ensure
      out.close
      writer.close
    end
  end

  def delete_bulk_import(name)
    begin
      @client.delete_bulk_import(name)
    rescue StandardError => e
      puts e.message
    end

  end

  def create_bulk_import(name, db, table)
    begin
      @client.create_bulk_import(name, db, table)
    rescue StandardError => e
      puts e.message
    end
  end

  def perform_bulk_import(name)
    put_title "Bulk import upload part"

    str_io = StringIO.new
    writer = Zlib::GzipWriter.new(str_io)
    writer.sync = true

    packer = MessagePack::Packer.new(writer)

    begin
      (1..100).each { |i|
        obj = {
          "time" => "#{i}",
          "col1" => "value#{i}",
          "col2" => "value_2_#{i}"
        }
        packer.write obj
      }
      packer.flush
      writer.flush(Zlib::FINISH)

      @client.bulk_import_upload_part(name, "part_1", str_io, str_io.size)
      job = @client.perform_bulk_import(name)
      puts "Performing bulk import, job ID: #{job.job_id}"

      wait_job(job)

      result = @client.bulk_import(name)
      puts "Bulk Status: #{result.status}"
    rescue StandardError => e
      puts e.message
    ensure
      writer.close
    end
  end

  def query_example(db_name, q)
    put_title "Querying data"
    job = @client.query(db_name, q)
    wait_job(job)
  end

  def run
    bulk_name = "td_client_ruby_bulk_import"
    db_name = "client_ruby_test"
    table = "log1"

    #server_status

    #account_info

    #list_databases
    
    #create_database(db_name)

    #create_log_table(db_name, table)

    ## Update table schema with specific fields
    #field1 = TreasureData::Schema::Field.new("col1", "string")
    #field2 = TreasureData::Schema::Field.new("col2", "string")
    #schema = TreasureData::Schema.new([field1, field2])
    #update_schema(db_name, table, schema)

    ## Or from json
    #schema = TreasureData::Schema.new
    #schema.from_json({
      #col1: "string",
      #col2: "string"
    #})
    #update_schema(db_name, table, schema)

    #import_data(db_name, table)

    #query_example(db_name, "select * from log1")
    #create_bulk_import(bulk_name, db_name, table)
    #perform_bulk_import(bulk_name)

#    delete_database(db_name)
#    delete_bulk_import(bulk_name)
  end
end

options = {:verify => ENV["MY_CUSTOM_CERT"]}
api_key = ENV["TD_API_KEY"] ||= ""
ex = Example.new(api_key, opt=options)
ex.server_status
