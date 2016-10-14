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
      before { allow(job).to receive(:finished?) { false } }

      it 'do not call #job_result_raw' do
        expect(client).not_to receive(:job_result_raw)

        expect(job.result_raw(format, io)).to_not be
      end
    end

    context 'finished?' do
      before { allow(job).to receive(:finished?) { true } }

      it 'call #job_result_raw' do
        expect(client).to receive(:job_result_raw).with(job_id, format, io)

        job.result_raw(format, io)
      end
    end
  end

  describe '#wait' do
    let(:client) { Client.authenticate('user', 'password') }
    let(:job_id) { 12345678 }
    let(:job)    { Job.new(client, job_id, nil, nil) }

    def change_job_status(status)
      allow(client).to receive(:job_status).with(job_id).and_return(status)
    end

    before do
      change_job_status(Job::STATUS_QUEUED)
    end

    context 'without timeout' do
      it 'waits the job to be finished' do
        begin
          thread = Thread.start { job.wait }
          expect(thread).to be_alive
          change_job_status(Job::STATUS_SUCCESS)
          thread.join(1)
          expect(thread).to be_stop
        ensure
          thread.kill # just in case
        end
      end

      it 'calls a given block in every wait_interval second' do
        now = 1_400_000_000
        allow(self).to receive(:sleep){|arg| now += arg }
        allow(Process).to receive(:clock_gettime){ now }
        expect { |b|
          begin
            thread = Thread.start {
              job.wait(nil, 2, &b)
            }
            sleep 6
            change_job_status(Job::STATUS_SUCCESS)
            thread.join(1)
            expect(thread).to be_stop
          ensure
            thread.kill # just in case
          end
        }.to yield_control.at_least(2).at_most(3).times
      end
    end

    context 'with timeout' do
      context 'the job running time is too long' do
        it 'raise Timeout::Error' do
          expect {
            job.wait(0.1)
          }.to raise_error(Timeout::Error)
        end
      end

      it 'calls a given block in every wait_interval second, and timeout' do
        expect { |b|
          begin
            thread = Thread.start {
              job.wait(0.3, 0.1, &b)
            }
            expect{ thread.value }.to raise_error(Timeout::Error)
            expect(thread).to be_stop
          ensure
            thread.kill # just in case
          end
        }.to yield_control.at_least(2).times
      end
    end
  end
end
