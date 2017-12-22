require 'spec_helper'
require 'td/client/spec_resources'

describe 'Table API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  let :client do
    Client.new(apikey)
  end

  describe "'create_log_table' API" do
    it 'should return 404 error if the database does not exist' do
      err_msg = "Create log table failed: Couldn't find UserDatabase with name = #{db_name}"
      stub_api_request(:post, "/v3/table/create/#{e db_name}/#{e(table_name)}/log").
        to_return(:status => 404, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_log_table(db_name, table_name)
      }.to raise_error(TreasureData::NotFoundError, /#{err_msg}/)
    end

    it 'should create a new table if the database exists' do
      stub_api_request(:post, "/v3/table/create/#{e db_name}/#{e(table_name)}/log").
        to_return(:body => {'database' => db_name, 'table' => table_name, 'type' => 'log'}.to_json)
      expect(api.create_log_table(db_name, table_name)).to be true
    end

    it 'should create a new table with params' do
      stub_api_request(:post, "/v3/table/create/#{e db_name}/#{e(table_name)}/log").
        with(:body => {'include_v' => 'false'}).
        to_return(:body => {'database' => db_name, 'table' => table_name, 'type' => 'log', 'include_v' => 'false'}.to_json)
      expect(api.create_log_table(db_name, table_name, include_v: false)).to be true
    end

    it 'should return 400 error with invalid name' do
      invalid_name = 'a'
      err_msg = "Name must be 3 to 256 characters, got #{invalid_name.length} characters. name = '#{invalid_name}'"
      stub_api_request(:post, "/v3/table/create/#{e db_name}/#{e invalid_name}/log").
        to_return(:status => 400, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_log_table(db_name, invalid_name)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    it 'should return 409 error with duplicated name' do
      err_msg = "Table #{table_name} already exists"
      stub_api_request(:post, "/v3/table/create/#{e db_name}/#{e table_name}/log").
        to_return(:status => 409, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_log_table(db_name, table_name)
      }.to raise_error(TreasureData::AlreadyExistsError, /#{err_msg}/)
    end
  end

  describe "'create_log_table' client API" do
    it 'should return 404 error if the database does not exist' do
      err_msg = "Create log table failed: Couldn't find UserDatabase with name = #{db_name}"
      stub_api_request(:post, "/v3/table/create/#{e db_name}/#{e(table_name)}/log").
        to_return(:status => 404, :body => {'message' => err_msg}.to_json)

      expect {
        client.create_log_table(db_name, table_name)
      }.to raise_error(TreasureData::NotFoundError, /#{err_msg}/)
    end

    it 'should create a new table if the database exists' do
      stub_api_request(:post, "/v3/table/create/#{e db_name}/#{e(table_name)}/log").
        to_return(:body => {'database' => db_name, 'table' => table_name, 'type' => 'log'}.to_json)
      expect(client.create_log_table(db_name, table_name)).to be true
    end

    it 'should create a new table with params' do
      stub_api_request(:post, "/v3/table/create/#{e db_name}/#{e(table_name)}/log").
        with(:body => {'include_v' => 'false'}).
        to_return(:body => {'database' => db_name, 'table' => table_name, 'type' => 'log', 'include_v' => 'false'}.to_json)
      expect(client.create_log_table(db_name, table_name, include_v: false)).to be true
    end

    it 'should return 400 error with invalid name' do
      invalid_name = 'a'
      err_msg = "Name must be 3 to 256 characters, got #{invalid_name.length} characters. name = '#{invalid_name}'"
      stub_api_request(:post, "/v3/table/create/#{e db_name}/#{e invalid_name}/log").
        to_return(:status => 400, :body => {'message' => err_msg}.to_json)

      expect {
        client.create_log_table(db_name, invalid_name)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    it 'should return 409 error with duplicated name' do
      err_msg = "Table #{table_name} already exists"
      stub_api_request(:post, "/v3/table/create/#{e db_name}/#{e table_name}/log").
        to_return(:status => 409, :body => {'message' => err_msg}.to_json)

      expect {
        client.create_log_table(db_name, table_name)
      }.to raise_error(TreasureData::AlreadyExistsError, /#{err_msg}/)
    end
  end

  describe "'list_tables' API" do
    it 'should list the tables in a Hash whose values include type, count, created_at, updated_at, schema, ...' do
      tables = [
        ["table_1", "item", "[[\"time\",\"long\"],[\"value\",\"string\"]]", 111, "2013-01-21 01:51:41 UTC", "2014-01-21 01:51:41 UTC"],
        ["table_2", "log",  "[[\"time\",\"long\"],[\"value\",\"long\"]]",   222, "2013-02-22 02:52:42 UTC", "2014-02-22 02:52:42 UTC"],
        ["table_3", "item", "[[\"time\",\"long\"],[\"value\",\"string\"]]", 333, "2013-03-23 03:53:43 UTC", "2014-03-23 03:53:43 UTC"],
        ["table_4", "log",  "[[\"time\",\"long\"],[\"value\",\"long\"]]",   444, "2013-04-24 04:54:44 UTC", "2014-04-24 04:54:44 UTC"]
      ]
      stub_api_request(:get, "/v3/table/list/#{e db_name}").
        to_return(:body => {'tables' => [
          {'name' => tables[0][0], 'type' => tables[0][1], 'schema' => tables[0][2], 'count' => tables[0][3], 'created_at' => tables[0][4], 'updated_at' => tables[0][5]},
          {'name' => tables[1][0], 'type' => tables[1][1], 'schema' => tables[1][2], 'count' => tables[1][3], 'created_at' => tables[1][4], 'updated_at' => tables[1][5]},
          {'name' => tables[2][0], 'type' => tables[2][1], 'schema' => tables[2][2], 'count' => tables[2][3], 'created_at' => tables[2][4], 'updated_at' => tables[2][5]},
          {'name' => tables[3][0], 'type' => tables[3][1], 'schema' => tables[3][2], 'count' => tables[3][3], 'created_at' => tables[3][4], 'updated_at' => tables[3][5]}
        ]}.to_json)

      table_list = api.list_tables(db_name)
      tables.each {|table|
        expect(table_list[table[0]][0]).to eq(table[1].to_sym)
        expect(table_list[table[0]][1]).to eq(JSON.parse(table[2]))
        expect(table_list[table[0]][2]).to eq(table[3])
        expect(table_list[table[0]][3]).to eq(table[4])
        expect(table_list[table[0]][4]).to eq(table[5])
      }
    end
  end

  describe "'tables' Client API" do
    it 'should return an array of Table objects' do
      tables = [
        ["table_1", "item", "[[\"value\",\"string\"]]", 111, "2013-01-21 01:51:41 UTC", "2014-01-21 01:51:41 UTC"],
        ["table_2", "log",  "[[\"value\",\"long\"]]",   222, "2013-02-22 02:52:42 UTC", "2014-02-22 02:52:42 UTC"],
        ["table_3", "item", "[[\"value\",\"string\"]]", 333, "2013-03-23 03:53:43 UTC", "2014-03-23 03:53:43 UTC"],
        ["table_4", "log",  "[[\"value\",\"long\"]]",   444, "2013-04-24 04:54:44 UTC", "2014-04-24 04:54:44 UTC"]
      ]
      stub_api_request(:get, "/v3/table/list/#{e db_name}").
        to_return(:body => {'tables' => [
          {'name' => tables[0][0], 'type' => tables[0][1], 'schema' => tables[0][2], 'count' => tables[0][3], 'created_at' => tables[0][4], 'updated_at' => tables[0][5]},
          {'name' => tables[1][0], 'type' => tables[1][1], 'schema' => tables[1][2], 'count' => tables[1][3], 'created_at' => tables[1][4], 'updated_at' => tables[1][5]},
          {'name' => tables[2][0], 'type' => tables[2][1], 'schema' => tables[2][2], 'count' => tables[2][3], 'created_at' => tables[2][4], 'updated_at' => tables[2][5]},
          {'name' => tables[3][0], 'type' => tables[3][1], 'schema' => tables[3][2], 'count' => tables[3][3], 'created_at' => tables[3][4], 'updated_at' => tables[3][5]}
        ]}.to_json)

      table_list = client.tables(db_name).sort_by { |e| e.name }

      db_count = 0
      tables.each {|table|
        db_count += table[3]
      }

      # REST API call to fetch the database permission
      stub_api_request(:get, "/v3/database/list").
        to_return(:body => {'databases' => [
          {'name' => db_name, 'count' => db_count, 'created_at' => tables[0][4], 'updated_at' => tables[0][5], 'permission' => 'full_access'}
        ]}.to_json)

      tables.length.times {|i|
        expect(table_list[i].db_name).to        eq(db_name)
        expect(table_list[i].name).to           eq(tables[i][0])
        expect(table_list[i].type).to           eq(tables[i][1].to_sym)
        expect(table_list[i].schema.to_json).to eq(eval(tables[i][2]).to_json)
        expect(table_list[i].count).to          eq(tables[i][3])
        expect(table_list[i].created_at).to     eq(Time.parse(tables[i][4]))
        expect(table_list[i].updated_at).to     eq(Time.parse(tables[i][5]))

        # REST API call to fetch the database permission
        stub_api_request(:get, "/v3/database/list").
          to_return(:body => {'databases' => [
            {'name' => db_name, 'count' => db_count, 'created_at' => tables[0][4], 'updated_at' => tables[0][5], 'permission' => 'full_access'}
          ]}.to_json)
        expect(table_list[i].permission).to eq(:full_access)

        # set up a trap to check this call never happens
        # - if it did, the next assertion on the count would fail
        stub_api_request(:get, "/v3/database/list").
          to_return(:body => {'databases' => [
            {'name' => db_name, 'count' => db_count + 100, 'created_at' => tables[0][4], 'updated_at' => tables[0][5], 'permission' => 'full_access'}
          ]}.to_json)
        expect(table_list[i].database.count).to eq(db_count)
      }
    end
  end

  describe "'table' Client API" do
    it 'should return the Table object corresponding to the name' do
      tables = [
        ["table_1", "item", "[[\"value\",\"string\"]]", 111, "2013-01-21 01:51:41 UTC", "2014-01-21 01:51:41 UTC"],
        ["table_2", "log",  "[[\"value\",\"long\"]]",   222, "2013-02-22 02:52:42 UTC", "2014-02-22 02:52:42 UTC"],
        ["table_3", "item", "[[\"value\",\"string\"]]", 333, "2013-03-23 03:53:43 UTC", "2014-03-23 03:53:43 UTC"],
        ["table_4", "log",  "[[\"value\",\"long\"]]",   444, "2013-04-24 04:54:44 UTC", "2014-04-24 04:54:44 UTC"]
      ]
      stub_api_request(:get, "/v3/table/list/#{e db_name}").
        to_return(:body => {'tables' => [
          {'name' => tables[0][0], 'type' => tables[0][1], 'schema' => tables[0][2], 'count' => tables[0][3], 'created_at' => tables[0][4], 'updated_at' => tables[0][5]},
          {'name' => tables[1][0], 'type' => tables[1][1], 'schema' => tables[1][2], 'count' => tables[1][3], 'created_at' => tables[1][4], 'updated_at' => tables[1][5]},
          {'name' => tables[2][0], 'type' => tables[2][1], 'schema' => tables[2][2], 'count' => tables[2][3], 'created_at' => tables[2][4], 'updated_at' => tables[2][5]},
          {'name' => tables[3][0], 'type' => tables[3][1], 'schema' => tables[3][2], 'count' => tables[3][3], 'created_at' => tables[3][4], 'updated_at' => tables[3][5]}
        ]}.to_json)

      i = 1
      table = client.table(db_name, tables[i][0])

      expect(table.name).to           eq(tables[i][0])
      expect(table.type).to           eq(tables[i][1].to_sym)
      expect(table.schema.to_json).to eq(eval(tables[i][2]).to_json)
      expect(table.count).to          eq(tables[i][3])
      expect(table.created_at).to     eq(Time.parse(tables[i][4]))
      expect(table.updated_at).to     eq(Time.parse(tables[i][5]))
    end
  end

  describe 'swap_table' do
    it 'should swap tables' do
      stub_api_request(:post, '/v3/table/swap/db/table1/table2')
      expect(api.swap_table('db', 'table1', 'table2')).to eq(true)
    end
  end

  describe 'update_expire' do
    it 'should update expiry days' do
      stub_api_request(:post, '/v3/table/update/db/table').
        with(:body => {'expire_days' => '5'}).
        to_return(:body => {'type' => 'type'}.to_json)
      expect(api.update_expire('db', 'table', 5)).to eq(true)
    end
  end

  describe 'handle include_v' do
    it 'should set/unset include_v flag' do
      stub_api_request(:get, '/v3/table/list/db').
        to_return(:body => {'tables' => [
          {'name' => 'table', 'type' => 'log', 'include_v' => true},
        ]}.to_json)

      table = client.table('db', 'table')
      expect(table.include_v).to eq true

      stub_api_request(:get, '/v3/table/list/db').
        to_return(:body => {'tables' => [
          {'name' => 'table', 'type' => 'log', 'include_v' => false},
        ]}.to_json)

      stub_api_request(:post, '/v3/table/update/db/table').
        with(:body => {'include_v' => "false"}).
        to_return(:body => {"database"=>"db","table"=>"table","type"=>"log"}.to_json)
      api.update_table('db', 'table', include_v: "false")

      table = client.table('db', 'table')
      expect(table.include_v).to eq false
    end
  end

  describe 'tail' do
    let :packed do
      s = StringIO.new
      pk = MessagePack::Packer.new(s)
      pk.write([1, 2, 3])
      pk.write([4, 5, 6])
      pk.flush
      s.string
    end

    it 'yields row if block given' do
      stub_api_request(:get, '/v3/table/tail/db/table').
        with(:query => {'format' => 'msgpack', 'count' => '10'}).
        to_return(:body => packed)
      result = []
      api.tail('db', 'table', 10) do |row|
        result << row
      end
      expect(result).to eq([[1, 2, 3], [4, 5, 6]])
    end

    it 'returns rows' do
      stub_api_request(:get, '/v3/table/tail/db/table').
        with(:query => {'format' => 'msgpack', 'count' => '10'}).
        to_return(:body => packed)
      expect(api.tail('db', 'table', 10)).to eq([[1, 2, 3], [4, 5, 6]])
    end

    it 'shows deprecated warning for from and to' do
      stub_api_request(:get, '/v3/table/tail/db/table').
        with(:query => {'format' => 'msgpack', 'count' => '10'}).
        to_return(:body => packed)
      r, w = IO.pipe
      begin
        backup = $stderr.dup
        $stderr.reopen(w)
        api.tail('db', 'table', 10, 100, 0)
      ensure
        $stderr.reopen(backup)
        w.close
      end
      expect(r.read).to eq(%Q(parameter "to" and "from" no longer work\n))
    end
  end
end
