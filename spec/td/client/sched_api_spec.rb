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
      stub_api_request(:post, "/v3/schedule/create/#{e(sched_name)}").
        with(:body => opts.merge('type' => 'hive')).
        to_return(:body => {'name' => sched_name, 'start' => start.to_s}.to_json)

      expect(api.create_schedule(sched_name, opts.merge('type' => 'hive'))).to eq(start.to_s)
    end

    it 'should create a dummy schedule' do
      stub_api_request(:post, "/v3/schedule/create/#{e(sched_name)}").
        with(:body => opts.merge('type' => 'hive')).
        to_return(:body => {'name' => sched_name, 'start' => nil}.to_json)

      expect(api.create_schedule(sched_name, opts.merge('type' => 'hive'))).to be_nil
    end

    it 'should return 422 error with invalid name' do
      name = '1'
      err_msg = "Validation failed: Name is too short" # " (minimum is 3 characters)"
      stub_api_request(:post, "/v3/schedule/create/#{e(name)}").
        with(:body => opts.merge('type' => 'hive')).
        to_return(:status => 422, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_schedule(name, opts.merge('type' => 'hive'))
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end
  end

  describe 'delete_schedule' do
    it 'should delete the schedule' do
      stub_api_request(:post, "/v3/schedule/delete/#{e(sched_name)}").
        to_return(:body => {'cron' => 'cron', 'query' => 'query'}.to_json)
      expect(api.delete_schedule(sched_name)).to eq(['cron', 'query'])
    end
  end

  describe 'update_schedule' do
    let :pig_query do
      "OUT = FOREACH (GROUP plt364 ALL) GENERATE COUNT(plt364);\n" * 200
    end
    let :opts do
      {'cron' => cron, 'query' => pig_query, 'database' => db_name}
    end

    it 'should not return 414 even if the query text is very long' do
      stub_api_request(:post, "/v3/schedule/update/#{e(sched_name)}").
        with(:body => opts.merge('type' => 'pig')).
        to_return(:body => {'name' => sched_name, 'query' => pig_query}.to_json)

      expect {
        api.update_schedule(sched_name, opts.merge('type' => 'pig'))
      }.not_to raise_error
    end

    it 'should update the schedule with the new query' do
      stub_api_request(:post, "/v3/schedule/update/#{e(sched_name)}").
        with(:body => opts.merge('type' => 'pig')).
        to_return(:body => {'name' => sched_name, 'query' => pig_query}.to_json)

      stub_api_request(:get, "/v3/schedule/list").
        to_return(:body => {'schedules' => [{'name' => sched_name, 'query' => pig_query}]}.to_json)

      expect(api.list_schedules.first[2]).to eq(pig_query)
    end
  end

  describe 'history' do
    let :history do
      ['history', 'job_id', 'type', 'database', 'status', 'query', 'start_at', 'end_at', 'result', 'priority'].inject({}) { |r, e|
        r[e] = e
        r
      }
    end

    it 'should return history records' do
      stub_api_request(:get, "/v3/schedule/history/#{e(sched_name)}").
        with(:query => {'from' => 0, 'to' => 100}).
        to_return(:body => {'history' => [history]}.to_json)
        expect(api.history(sched_name, 0, 100)).to eq([[nil, 'job_id', :type, 'status', 'query', 'start_at', 'end_at', 'result', 'priority', 'database']])
    end
  end

  describe 'run_schedule' do
    it 'should return history records' do
      stub_api_request(:post, "/v3/schedule/run/#{e(sched_name)}/123456789").
        with(:body => {'num' => '5'}).
        to_return(:body => {'jobs' => [{'job_id' => 'job_id', 'scheduled_at' => 'scheduled_at', 'type' => 'type'}]}.to_json)
        expect(api.run_schedule(sched_name, 123456789, 5)).to eq([['job_id', :type, 'scheduled_at']])
    end
  end
end
