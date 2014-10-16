require 'spec_helper'
require 'td/client/spec_resources'

describe 'Export API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  describe 'export' do
    let :storage_type do
      's3'
    end

    it 'should export successfully' do
      # TODO: Use correnty values
      params = {'file_format' => 'json.gz', 'bucket' => 'bin', 'access_key_id' => 'id', 'secret_access_key' => 'secret'}
      stub_api_request(:post, "/v3/export/run/#{e(db_name)}/#{e(table_name)}").with(:body => params.merge('storage_type' => storage_type)).
        to_return(:body => {'database' => db_name, 'job_id' => '1', 'debug' => {}}.to_json)

      api.export(db_name, table_name, storage_type, params).should == '1'
    end

    it 'should return 400 error with invalid storage type' do
      invalid_type = 'gridfs'
      params = {'storage_type' => invalid_type}
      err_msg = "Only s3 output type is supported: #{invalid_type}"
      stub_api_request(:post, "/v3/export/run/#{e(db_name)}/#{e(table_name)}").with(:body => params).
        to_return(:status => 400, :body => {'message' => err_msg}.to_json)

      expect {
        api.export(db_name, table_name, invalid_type)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    # TODO: Add other parameters spec
  end
end

