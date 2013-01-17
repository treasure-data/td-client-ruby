require 'spec_helper'
require 'td/client/spec_resources'

describe 'Schedule API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  describe 'create_schedule' do
    let :opts do
      {'cron' => cron, 'query' => query, 'database' => db_name}
    end

    it 'should create a new schedule' do
      start = Time.now
      stub_api_request(:post, "/v3/schedule/create/#{e(sched_name)}").with(opts.merge('type' => 'hive'))
        .to_return(:body => {'name' => sched_name, 'start' => start.to_s}.to_json)

      api.create_schedule(sched_name, opts).should == start.to_s
    end

    it 'should return 422 error with invalid name' do
      name = '1'
      err_msg = "Validation failed: Name is too short" # " (minimum is 3 characters)"
      stub_api_request(:post, "/v3/schedule/create/#{e(name)}").with(opts.merge('type' => 'hive'))
        .to_return(:status => 422, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_schedule(name, opts)
      }.to raise_error(TreasureData::APIError, /^#{err_msg}/)
    end
  end
end
