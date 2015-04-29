require 'spec_helper'
require 'td/client/spec_resources'

describe 'Job Model' do
  include_context 'spec symbols'
  include_context 'common helper'
  include_context 'job resources'

  before do
    stub_api_request(:post, "/v3/user/authenticate").
      to_return(:body => {'apikey' => 'apikey'}.to_json)
  end

  describe '#client' do
    subject do
      Job.new(client, *arguments).client
    end

    let :client do
      Client.authenticate('user', 'password')
    end

    let :arguments do
      job_attributes = raw_jobs.first
      [
        'job_id', 'type', 'query', 'status', 'url', 'debug',
        'start_at', 'end_at', 'cpu_time', 'result_size', 'result', 'result_url',
        'hive_result_schema', 'priority', 'retry_limit', 'org_name', 'db_name',
        'duration'
      ].map {|name| job_attributes[name]}
    end

    it 'returns Job object having client' do
      expect(subject).to eq client
    end
  end
end
