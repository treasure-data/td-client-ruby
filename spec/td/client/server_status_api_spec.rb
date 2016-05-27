require 'spec_helper'
require 'td/client/spec_resources'

describe 'ServerStatus API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil, {:max_cumul_retry_delay => -1})
  end

  describe 'server_status' do
    it 'returns status' do
      stub_api_request(:get, '/v3/system/server_status').
        to_return(:body => {'status' => 'OK'}.to_json)
      expect(api.server_status).to eq('OK')
    end

    it 'returns error description' do
      stub_api_request(:get, '/v3/system/server_status').
        to_return(:status => 500)
      expect(api.server_status).to eq('Server is down (500)')
    end
  end
end
