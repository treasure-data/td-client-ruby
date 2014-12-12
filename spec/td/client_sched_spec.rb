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

  describe 'history' do
    let :opts do
      {'database' => db_name}
    end

    let :history do
      ['history', 'job_id', 'type', 'database', 'status', 'query', 'start_at', 'end_at', 'result', 'priority'].inject({}) { |r, e|
        r[e] = e
        r
      }
    end

    it 'returns scheduled_job' do
      stub_api_request(:get, "/v3/schedule/history/#{e(sched_name)}?from=0&to=19").
        to_return(:body => {'count' => 1, 'history' => [history]}.to_json)

      client.history(sched_name, 0, 19).each do |scheduled_job|
        scheduled_job.job_id.should == 'job_id'
        scheduled_job.status.should == 'status'
        scheduled_job.priority.should == 'priority'
        scheduled_job.result_url.should == 'result'
      end
    end
  end
end
