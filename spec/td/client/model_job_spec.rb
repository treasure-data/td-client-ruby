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

  describe '#result_raw' do
    let(:client) { Client.authenticate('user', 'password') }
    let(:job_id) { 12345678 }
    let(:job)    { Job.new(client, job_id, nil, nil) }
    let(:format) { 'json' }
    let(:io)     { StringIO.new }

    context 'not finished?' do
      before { job.stub(:finished?) { false } }

      it 'do not call #job_result_raw' do
        client.should_not_receive(:job_result_raw)

        expect(job.result_raw(format, io)).to_not be
      end
    end

    context 'finished?' do
      before { job.stub(:finished?) { true } }

      it 'call #job_result_raw' do
        client.should_receive(:job_result_raw).with(job_id, format, io)

        job.result_raw(format, io)
      end
    end
  end
end
