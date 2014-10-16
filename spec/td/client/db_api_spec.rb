require 'spec_helper'
require 'td/client/spec_resources'

describe 'Database API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  let :client do
    Client.new(apikey)
  end

  describe "'create_database' API" do
    it 'should create a new database' do
      stub_api_request(:post, "/v3/database/create/#{e(db_name)}").
        to_return(:body => {'database' => db_name}.to_json)

      api.create_database(db_name).should be true
    end

    it 'should return 400 error with invalid name' do
      invalid_name = 'a'
      err_msg = "Name must be 3 to 256 characters, got #{invalid_name.length} characters. name = '#{invalid_name}'"
      stub_api_request(:post, "/v3/database/create/#{e(invalid_name)}").
        to_return(:status => 400, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_database(invalid_name)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    it 'should return 409 error with duplicated name' do
      err_msg = "Database #{db_name} already exists"
      stub_api_request(:post, "/v3/database/create/#{e(db_name)}").
        to_return(:status => 409, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_database(db_name)
      }.to raise_error(TreasureData::AlreadyExistsError, /#{err_msg}/)
    end
  end

  describe "'list_databases' API" do
    it 'should list the databases with count, created_at, updated_at, organization, and permission' do
      databases = [
        ["db_1", 111, "2013-01-21 01:51:41 UTC", "2014-01-21 01:51:41 UTC", nil, "administrator"],
        ["db_2", 222, "2013-02-22 02:52:42 UTC", "2014-02-22 02:52:42 UTC", nil, "full_access"],
        ["db_3", 333, "2013-03-23 03:53:43 UTC", "2014-03-23 03:53:43 UTC", nil, "import_only"],
        ["db_4", 444, "2013-04-24 04:54:44 UTC", "2014-04-24 04:54:44 UTC", nil, "query_only"]
      ]
      stub_api_request(:get, "/v3/database/list").
        to_return(:body => {'databases' => [
          {'name' => databases[0][0], 'count' => databases[0][1], 'created_at' => databases[0][2], 'updated_at' => databases[0][3], 'organization' => databases[0][4], 'permission' => databases[0][5]},
          {'name' => databases[1][0], 'count' => databases[1][1], 'created_at' => databases[1][2], 'updated_at' => databases[1][3], 'organization' => databases[1][4], 'permission' => databases[1][5]},
          {'name' => databases[2][0], 'count' => databases[2][1], 'created_at' => databases[2][2], 'updated_at' => databases[2][3], 'organization' => databases[2][4], 'permission' => databases[2][5]},
          {'name' => databases[3][0], 'count' => databases[3][1], 'created_at' => databases[3][2], 'updated_at' => databases[3][3], 'organization' => databases[3][4], 'permission' => databases[3][5]}
        ]}.to_json)

      db_list = api.list_databases
      databases.each {|db|
        expect(db_list[db[0]]).to eq(db[1..-1])
      }
    end
  end

  describe "'databases' Client API" do
    it 'should return an array of Databases objects containing name, count, created_at, updated_at, organization, and permission' do
      databases = [
        ["db_1", 111, "2013-01-21 01:51:41 UTC", "2014-01-21 01:51:41 UTC", nil, "administrator"],
        ["db_2", 222, "2013-02-22 02:52:42 UTC", "2014-02-22 02:52:42 UTC", nil, "full_access"],
        ["db_3", 333, "2013-03-23 03:53:43 UTC", "2014-03-23 03:53:43 UTC", nil, "import_only"],
        ["db_4", 444, "2013-04-24 04:54:44 UTC", "2014-04-24 04:54:44 UTC", nil, "query_only"]
      ]
      stub_api_request(:get, "/v3/database/list").
        to_return(:body => {'databases' => [
          {'name' => databases[0][0], 'count' => databases[0][1], 'created_at' => databases[0][2], 'updated_at' => databases[0][3], 'organization' => databases[0][4], 'permission' => databases[0][5]},
          {'name' => databases[1][0], 'count' => databases[1][1], 'created_at' => databases[1][2], 'updated_at' => databases[1][3], 'organization' => databases[1][4], 'permission' => databases[1][5]},
          {'name' => databases[2][0], 'count' => databases[2][1], 'created_at' => databases[2][2], 'updated_at' => databases[2][3], 'organization' => databases[2][4], 'permission' => databases[2][5]},
          {'name' => databases[3][0], 'count' => databases[3][1], 'created_at' => databases[3][2], 'updated_at' => databases[3][3], 'organization' => databases[3][4], 'permission' => databases[3][5]}
        ]}.to_json)

      db_list = client.databases.sort_by { |e| e.name }
      databases.length.times {|i|
        expect(db_list[i].name).to          eq(databases[i][0])
        expect(db_list[i].count).to         eq(databases[i][1])
        expect(db_list[i].created_at).to    eq(Time.parse(databases[i][2]))
        expect(db_list[i].updated_at).to    eq(Time.parse(databases[i][3]))
        expect(db_list[i].org_name).to      eq(databases[i][4])
        expect(db_list[i].permission).to    eq(databases[i][5].to_sym)
      }
    end
  end

  describe "'database' Client API" do
    it "should return the Databases object corresponding to the name and containing count, created_at, updated_at, organization, and permission" do
      databases = [
        ["db_1", 111, "2013-01-21 01:51:41 UTC", "2014-01-21 01:51:41 UTC", nil, "administrator"],
        ["db_2", 222, "2013-02-22 02:52:42 UTC", "2014-02-22 02:52:42 UTC", nil, "full_access"],
        ["db_3", 333, "2013-03-23 03:53:43 UTC", "2014-03-23 03:53:43 UTC", nil, "import_only"],
        ["db_4", 444, "2013-04-24 04:54:44 UTC", "2014-04-24 04:54:44 UTC", nil, "query_only"]
      ]
      stub_api_request(:get, "/v3/database/list").
        to_return(:body => {'databases' => [
          {'name' => databases[0][0], 'count' => databases[0][1], 'created_at' => databases[0][2], 'updated_at' => databases[0][3], 'organization' => databases[0][4], 'permission' => databases[0][5]},
          {'name' => databases[1][0], 'count' => databases[1][1], 'created_at' => databases[1][2], 'updated_at' => databases[1][3], 'organization' => databases[1][4], 'permission' => databases[1][5]},
          {'name' => databases[2][0], 'count' => databases[2][1], 'created_at' => databases[2][2], 'updated_at' => databases[2][3], 'organization' => databases[2][4], 'permission' => databases[2][5]},
          {'name' => databases[3][0], 'count' => databases[3][1], 'created_at' => databases[3][2], 'updated_at' => databases[3][3], 'organization' => databases[3][4], 'permission' => databases[3][5]}
        ]}.to_json)

      i = 1
      db = client.database(databases[i][0])
      expect(db.name).to        eq(databases[i][0])
      expect(db.count).to       eq(databases[i][1])
      expect(db.created_at).to  eq(Time.parse(databases[i][2]))
      expect(db.updated_at).to  eq(Time.parse(databases[i][3]))
      expect(db.org_name).to    eq(databases[i][4])
      expect(db.permission).to  eq(databases[i][5].to_sym)
    end
  end
end
