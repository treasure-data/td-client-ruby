require 'spec_helper'
require 'td/client/spec_resources'

describe 'Schedule API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  let :client do
    Client.new(apikey)
  end

  describe "'list_schedules' API" do
    it 'should list the schedules' do
      schedules = [
        {
            "name": "1 id 2 list",
            "cron": nil,
            "query": "SELECT '17775792' as id, 1 as list_3296, 1 as list_13602 ",
            "database": "linh_sf_pardot",
            "result": "{\"type\":\"salesforce_pardot\",\"login_url\":\"https://login.salesforce.com\",\"pardot_domain\":\"https://pi.pardot.com\",\"business_unit\":\"0Uv4W0000008OOISA2\",\"data_object\":\"prospects\",\"prospect_operation\":\"sync\",\"prospect_list_operation\":\"add\",\"list_membership_operation\":\"upsert\",\"skip_invalid_records\":true}",
            "timezone": "UTC",
            "delay": 0,
            "next_time": nil,
            "priority": 0,
            "retry_limit": 0,
            "organization": nil, # for the test, this prop is not in the response
            "id": 91618,
            "description": nil,
            "executing_user_id": 602
        },
        {
            "name": "111_gs_one_drive_personal clone111",
            "cron": nil,
            "query": "select * from 111222",
            "database": nil,
            "result": "",
            "timezone": "UTC",
            "delay": 0,
            "next_time": nil,
            "priority": 2,
            "retry_limit": 0,
            "organization": nil, # for the test, this prop is not in the response
            "id": 20106,
            "description": nil,
            "executing_user_id": 78
        },
        {
            "name": "111_gs_one_drive_personal clone111 clone",
            "cron": nil,
            "query": "select * from append_01knlfnlsdnfv;",
            "database": nil,
            "result": "",
            "timezone": "UTC",
            "delay": 0,
            "priority": 2,
            "retry_limit": 0,
            "next_time": nil,
            "organization": nil, # for the test, this prop is not in the response
            "id": 39377,
            "description": nil,
            "executing_user_id": 548,
        }
      ]

      result_schedules = schedules.map do |sched| 
        sched.values
      end

      stub_api_request(:get, "/v3/schedule/list").
        to_return(:body => {'schedules' => schedules}.to_json)

      schedule_list = api.list_schedules
      result_schedules.each_with_index do |sched, idx|
        expect(schedule_list[idx]).to match_array(sched)
      end 
    end
  end

  describe "'schedules' Client API" do
    it 'should return an array of Schedule objects' do
      schedules = [
        {
            "name": "1 id 2 list",
            "cron": nil,
            "query": "SELECT '17775792' as id, 1 as list_3296, 1 as list_13602 ",
            "database": "linh_sf_pardot",
            "result": "{\"type\":\"salesforce_pardot\",\"login_url\":\"https://login.salesforce.com\",\"pardot_domain\":\"https://pi.pardot.com\",\"business_unit\":\"0Uv4W0000008OOISA2\",\"data_object\":\"prospects\",\"prospect_operation\":\"sync\",\"prospect_list_operation\":\"add\",\"list_membership_operation\":\"upsert\",\"skip_invalid_records\":true}",
            "timezone": "UTC",
            "delay": 0,
            "next_time": nil,
            "priority": 0,
            "retry_limit": 0,
            "organization": nil, # for the test, this prop is not in the response
            "id": 91618,
            "description": nil,
            "executing_user_id": 602
        },
        {
            "name": "111_gs_one_drive_personal clone111",
            "cron": nil,
            "query": "select * from 111222",
            "database": nil,
            "result": "",
            "timezone": "UTC",
            "delay": 0,
            "next_time": nil,
            "priority": 2,
            "retry_limit": 0,
            "organization": nil, # for the test, this prop is not in the response
            "id": 20106,
            "description": nil,
            "executing_user_id": 78
        },
        {
            "name": "111_gs_one_drive_personal clone111 clone",
            "cron": nil,
            "query": "select * from append_01knlfnlsdnfv;",
            "database": nil,
            "result": "",
            "timezone": "UTC",
            "delay": 0,
            "priority": 2,
            "retry_limit": 0,
            "next_time": nil,
            "organization": nil, # for the test, this prop is not in the response
            "id": 39377,
            "description": nil,
            "executing_user_id": 548,
        }
      ]

      result_schedules = schedules.map do |sched| 
        sched.values
      end

      stub_api_request(:get, "/v3/schedule/list").
        to_return(:body => {'schedules' => schedules}.to_json)

      schedule_list = client.schedules
      schedule_list.each_with_index do |s, idx|
        expect(s.name).to eq(schedules[idx][:name])
        expect(s.cron).to eq(schedules[idx][:cron])
        expect(s.query).to eq(schedules[idx][:query])
        expect(s.database).to eq(schedules[idx][:database])
        expect(s.id).to eq(schedules[idx][:id])
        expect(s.executing_user_id).to eq(schedules[idx][:executing_user_id])
        expect(s.description).to eq(schedules[idx][:description])
      end
    end
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
