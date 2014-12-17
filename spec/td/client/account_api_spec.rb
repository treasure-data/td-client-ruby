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
      api.show_account.should == [1, 0, 2, 3, 4, "2014-12-14T17:24:00+0900"]
    end
  end

  describe 'account_core_utilization' do
    it 'returns core utilization' do
      from = '2014-12-01T00:00:00+0900'
      to = '2015-01-01T00:00:00+0900'
      stub_api_request(:get, "/v3/account/core_utilization", :query => {'from' => from, 'to' => to}).
        to_return(:body => {'from' => from, 'to' => to, 'interval' => 1, 'history' => ['dummy']}.to_json)
      r = api.account_core_utilization(from, to)
      r[0].should == Time.parse(from)
      r[1].should == Time.parse(to)
      r[2].should == 1
      r[3].should == ['dummy']
    end
  end
end
