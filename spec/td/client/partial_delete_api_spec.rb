require 'spec_helper'
require 'td/client/spec_resources'

describe 'PartialDelete API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  describe 'partialdelete' do
    let :from do
      0
    end

    let :to do
      3600 * 10
    end

    let :from_to do
      {'from' => from.to_s, 'to' => to.to_s}
    end

    it 'should partial_delete successfully' do
      # TODO: Use correnty values
      stub_api_request(:post, "/v3/table/partialdelete/#{e(db_name)}/#{e(table_name)}").with(:body => from_to).
        to_return(:body => {'database' => db_name, 'table' => table_name, 'job_id' => '1'}.to_json)

      api.partial_delete(db_name, table_name, to, from).should == '1'
    end

    it 'should return 404 error with non exist database name' do
      db = 'no_such_db'
      err_msg = "Couldn't find UserDatabase with name = #{db}"
      stub_api_request(:post, "/v3/table/partialdelete/#{e(db)}/#{e(table_name)}").with(:body => from_to).
        to_return(:status => 404, :body => {'message' => err_msg}.to_json)

      expect {
        api.partial_delete(db, table_name, to, from)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    it 'should return 404 error with non exist table name' do
      table = 'no_such_table'
      err_msg = "Unknown table: #{table}"
      stub_api_request(:post, "/v3/table/partialdelete/#{e(db_name)}/#{e(table)}").with(:body => from_to).
        to_return(:status => 404, :body => {'message' => err_msg}.to_json)

      expect {
        api.partial_delete(db_name, table, to, from)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    # TODO: Add from / to parameters spec
  end
end

