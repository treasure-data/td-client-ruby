require 'spec_helper'
require 'td/client/spec_resources'

describe 'BulkImport API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  describe 'create_bulk_import' do
    it 'should create a new bulk_import' do
      stub_api_request(:post, "/v3/bulk_import/create/#{e(bi_name)}/#{e(db_name)}/#{e(table_name)}").
        to_return(:body => {'bulk_import' => bi_name}.to_json)

      api.create_bulk_import(bi_name, db_name, table_name).should be_nil
    end

    it 'should return 422 error with invalid name' do
      name = 'D'
      err_msg = "Validation failed: Name is too short" # " (minimum is 3 characters)"
      stub_api_request(:post, "/v3/bulk_import/create/#{e(name)}/#{e(db_name)}/#{e(table_name)}").
        to_return(:status => 422, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_bulk_import(name, db_name, table_name)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    it 'should return 404 error with non exist database name' do
      db = 'no_such_db'
      err_msg = "Couldn't find UserDatabase with name = #{db}"
      stub_api_request(:post, "/v3/bulk_import/create/#{e(bi_name)}/#{e(db)}/#{e(table_name)}").
        to_return(:status => 404, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_bulk_import(bi_name, db, table_name)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    it 'should return 404 error with non exist table name' do
      table = 'no_such_table'
      err_msg = "Couldn't find UserTableReference with name = #{table}"
      stub_api_request(:post, "/v3/bulk_import/create/#{e(bi_name)}/#{e(db_name)}/#{e(table)}").
        to_return(:status => 404, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_bulk_import(bi_name, db_name, table)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end
  end
end
