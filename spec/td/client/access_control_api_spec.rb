require 'spec_helper'
require 'td/client/spec_resources'
require 'json'

describe 'Access Control API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    # no retry for GET
    API.new(nil, {:max_cumul_retry_delay => -1})
  end

  describe 'all apis' do
    it 'is deprecated' do
      stub_api_request(:post, "/v3/acl/grant").to_return(:status => 500)
      expect {
        api.grant_access_control('subject', 'action', 'scope', [])
      }.to raise_error(TreasureData::APIError)

      stub_api_request(:post, "/v3/acl/revoke").to_return(:status => 500)
      expect {
        api.revoke_access_control('subject', 'action', 'scope')
      }.to raise_error(TreasureData::APIError)

      stub_api_request(:get, "/v3/acl/test", :query => {'user' => 'user', 'action' => 'action', 'scope' => 'scope'}).to_return(:status => 422)
      expect {
        api.test_access_control('user', 'action', 'scope')
      }.to raise_error(TreasureData::APIError)

      stub_api_request(:get, "/v3/acl/list").to_return(:status => 500)
      expect {
        api.list_access_controls
      }.to raise_error(TreasureData::APIError)
    end
  end
end
