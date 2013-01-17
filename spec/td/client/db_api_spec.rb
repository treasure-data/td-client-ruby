require 'spec_helper'
require 'td/client/spec_resources'

describe 'Database API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  describe 'create_database' do
    it 'should create a new database' do
      stub_api_request(:post, "/v3/database/create/#{e(db_name)}").to_return(:body => {'database' => db_name}.to_json)

      api.create_database(db_name).should be_true
    end

    it 'should return 400 error with invalid name' do
      invalid_name = 'a'
      err_msg = "Name must be 3 to 256 characters, got #{invalid_name.length} characters. name = '#{invalid_name}'"
      stub_api_request(:post, "/v3/database/create/#{e(invalid_name)}").to_return(:status => 400, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_database(invalid_name)
      }.to raise_error(TreasureData::APIError, /^#{err_msg}/)
    end

    it 'should return 409 error with duplicated name' do
      err_msg = "Database #{db_name} already exists"
      stub_api_request(:post, "/v3/database/create/#{e(db_name)}").to_return(:status => 409, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_database(db_name)
      }.to raise_error(TreasureData::APIError, /^#{err_msg}/)
    end
  end
end
