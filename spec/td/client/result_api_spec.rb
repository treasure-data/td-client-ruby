require 'spec_helper'
require 'td/client/spec_resources'

describe 'Result API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  describe 'create_result' do
    it 'should create a new result' do
      params = {'url' => result_url}
      stub_api_request(:post, "/v3/result/create/#{e(result_name)}").with(:body => params).to_return(:body => {'result' => result_name}.to_json)

      expect(api.create_result(result_name, result_url)).to be true
    end

    it 'should return 422 error with invalid name' do
      name = '1'
      params = {'url' => result_url}
      err_msg = "Validation failed: Name is too short" # " (minimum is 3 characters)"
      stub_api_request(:post, "/v3/result/create/#{e(name)}").with(:body => params).
        to_return(:status => 422, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_result(name, result_url)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    it 'should return 422 error without url' do
      params = {'url' => 'false'} # I want to use nil, but nil doesn't work on WebMock...
      err_msg = "'url' parameter is required"
      stub_api_request(:post, "/v3/result/create/#{e(result_name)}").with(:body => params).
        to_return(:status => 422, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_result(result_name, false)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    it 'should return 409 error with duplicated name' do
      params = {'url' => result_url}
      err_msg = "Result must be unique"
      stub_api_request(:post, "/v3/result/create/#{e(result_name)}").with(:body => params).
        to_return(:status => 409, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_result(result_name, result_url)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end
  end

  describe 'list_result' do
    it 'should return name and url' do
      stub_api_request(:get, '/v3/result/list').
        to_return(:body => {'results' => [{'name' => 'name', 'url' => 'url'}]}.to_json)
      expect(api.list_result).to eq([['name', 'url', nil]])
    end
  end

  describe 'delete_result' do
    it 'should delete the result' do
      stub_api_request(:post, "/v3/result/delete/#{e(result_name)}")
      expect(api.delete_result(result_name)).to eq(true)
    end

    it 'should raise error' do
      stub_api_request(:post, "/v3/result/delete/#{e(result_name)}").
        to_return(:status => 404)
      expect {
        api.delete_result(result_name)
      }.to raise_error(TreasureData::APIError)
    end
  end
end
