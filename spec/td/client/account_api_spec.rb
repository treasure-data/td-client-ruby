require 'spec_helper'
require 'td/client/spec_resources'
require 'json'

describe 'Account API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  describe 'show_account' do
    it 'returns account properties' do
      stub_api_request(:get, "/v3/account/show").
        to_return(:body => {'account' => {'id' => 1, 'plan' => 0, 'storage_size' => 2, 'guaranteed_cores' => 3, 'maximum_cores' => 4, 'created_at' => '2014-12-14T17:24:00+0900'}}.to_json)
      expect(api.show_account).to eq([1, 0, 2, 3, 4, "2014-12-14T17:24:00+0900"])
    end
  end

  describe 'account_core_utilization' do
    it 'returns core utilization' do
      from = '2014-12-01T00:00:00+0900'
      to = '2015-01-01T00:00:00+0900'
      stub_api_request(:get, "/v3/account/core_utilization", :query => {'from' => from, 'to' => to}).
        to_return(:body => {'from' => from, 'to' => to, 'interval' => 1, 'history' => ['dummy']}.to_json)
      r = api.account_core_utilization(from, to)
      expect(r[0]).to eq(Time.parse(from))
      expect(r[1]).to eq(Time.parse(to))
      expect(r[2]).to eq(1)
      expect(r[3]).to eq(['dummy'])
    end
  end
end
