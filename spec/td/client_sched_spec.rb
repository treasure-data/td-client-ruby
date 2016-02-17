require 'spec_helper'
require 'td/client/spec_resources'

describe 'Schedule Command' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  let :client do
    client = TreasureData::Client.new('dummy')
    client.instance_variable_set('@api', api)
    client
  end

  describe 'create' do
    let :opts do
      {:database => db_name, :cron => '', :type => 'hive', :query => 'select 1;'}
    end

    before do
      stub_api_request(:post, "/v3/schedule/create/#{e(sched_name)}").
      with(:body => opts).
        to_return(:body => {'name' => sched_name, 'start' => start}.to_json)
    end
    context 'start is now' do
      let (:start){ Time.now.round }
      it 'returns Time object' do
        client.create_schedule(sched_name, opts).should == start
      end
    end

    context 'start is nil' do
      let (:start){ nil }
      it do
        client.create_schedule(sched_name, opts).should == start
      end
    end
  end

  describe 'history' do
    let :opts do
      {'database' => db_name}
    end

    let :history do
      ['history', 'scheduled_at', 'job_id', 'type', 'database', 'status', 'query', 'start_at', 'end_at', 'result', 'priority'].inject({}) { |r, e|
        r[e] = e
        r
      }
    end

    it 'returns scheduled_job' do
      h = history; h['scheduled_at'] = '2015-02-17 14:16:00 +0900'
      stub_api_request(:get, "/v3/schedule/history/#{e(sched_name)}?from=0&to=19").
        to_return(:body => {'count' => 1, 'history' => [h]}.to_json)

      client.history(sched_name, 0, 19).each do |scheduled_job|
        scheduled_job.scheduled_at.xmlschema.should == Time.parse('2015-02-17T14:16:00+09:00').xmlschema #avoid depending on CI's Locale
        scheduled_job.job_id.should == 'job_id'
        scheduled_job.status.should == 'status'
        scheduled_job.priority.should == 'priority'
        scheduled_job.result_url.should == 'result'
      end
    end

    it 'works when scheduled_at == ""' do
      h = history; h['scheduled_at'] = ''
      stub_api_request(:get, "/v3/schedule/history/#{e(sched_name)}?from=0&to=19").
        to_return(:body => {'count' => 1, 'history' => [h]}.to_json)

      client.history(sched_name, 0, 19).each do |scheduled_job|
        scheduled_job.scheduled_at.should == nil
      end
    end
  end
end
