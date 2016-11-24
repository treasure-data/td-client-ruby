require 'spec_helper'
require 'td/client/spec_resources'

describe 'Command' do
  include_context 'spec symbols'
  include_context 'common helper'
  include_context 'job resources'

  before do
    stub_api_request(:post, "/v3/user/authenticate").
      to_return(:body => {'apikey' => 'apikey'}.to_json)
  end

  let :client do
    Client.authenticate('user', 'password')
  end

  describe '#job' do
    before do
      stub_api_request(:get, "/v3/job/list").to_return(:body => {'jobs' => raw_jobs}.to_json)
    end

    it 'return jobs created with API result' do
      jobs = client.jobs

      expect(jobs).to be_kind_of Array
      jobs.each.with_index do |job, i|
        expect(job.job_id).to       eq raw_jobs[i]['job_id']
        expect(job.type).to         eq raw_jobs[i]['type']
        expect(job.status).to       eq raw_jobs[i]['status']
        expect(job.query).to        eq raw_jobs[i]['query']
        expect(job.start_at).to     eq Time.parse(raw_jobs[i]['start_at'])
        expect(job.end_at).to       eq Time.parse(raw_jobs[i]['end_at'])
        expect(job.cpu_time).to     eq raw_jobs[i]['cpu_time']
        expect(job.result_size).to  eq raw_jobs[i]['result_size']
        expect(job.result_url).to   eq raw_jobs[i]['result_url']
        expect(job.priority).to     eq raw_jobs[i]['priority']
        expect(job.retry_limit).to  eq raw_jobs[i]['retry_limit']
        expect(job.org_name).to     eq raw_jobs[i]['organization']
        expect(job.db_name).to      eq raw_jobs[i]['database']
        expect(job.duration).to     eq raw_jobs[i]['duration']
        expect(job.num_records).to  eq raw_jobs[i]['num_records']
      end
    end
  end
end
