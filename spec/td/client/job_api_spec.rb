require 'spec_helper'
require 'td/client/spec_resources'
require 'json'

describe 'Job API' do
  include_context 'spec symbols'
  include_context 'job resources'

  let :api do
    API.new(nil)
  end

  let :api_with_short_retry do
    API.new(nil, {:max_cumul_retry_delay => 10})
  end

  describe 'list_jobs' do
    it 'should returns 20 jobs by default' do
      stub_api_request(:get, "/v3/job/list", :query => {'from' => '0'}).to_return(:body => {'jobs' => raw_jobs}.to_json)
      jobs = api.list_jobs
      jobs.size.should == 20
    end

    (0...MAX_JOB).each {|i|
      it "should get the correct fields for job #{i} of 20" do
        job = raw_jobs[i]
        stub_api_request(:get, "/v3/job/list", :query => {'from' => '0'}).to_return(:body => {'jobs' => raw_jobs}.to_json)

        jobs = api.list_jobs
        jobs[i..i].map {|job_id, type, status, query, start_at, end_at, cpu_time,
                      result_size, result_url, priority, retry_limit, org, db|
          job_id.should == job['job_id']
          type.should == job['type']
          status.should == job['status']
          query.should == job['query']
          start_at.should == job['start_at']
          end_at.should == job['end_at']
          cpu_time.should == job['cpu_time']
          result_size.should == job['result_size']
          result_url.should == job['result_url']
          priority.should == job['priority']
          retry_limit.should == job['retry_limit']
          org.should == job['organization']
          db.should == job['database']
        }
      end
    }

    it 'should returns 10 jobs with to parameter' do
      stub_api_request(:get, "/v3/job/list", :query => {'from' => '0', 'to' => '10'}).to_return(:body => {'jobs' => raw_jobs[0...10]}.to_json)
      jobs = api.list_jobs(0, 10)
      jobs.size.should == 10
    end

    it 'should returns 10 jobs with to status parameter' do
      error_jobs = raw_jobs.select { |j| j['status'] == 'error' }
      stub_api_request(:get, "/v3/job/list", :query => {'from' => '0', 'status' => 'error'}).to_return(:body => {'jobs' => error_jobs}.to_json)
      jobs = api.list_jobs(0, nil, 'error')
      jobs.size.should == error_jobs.size
    end

    #it 'should contain the result_size field' do

  end

  describe 'show_job' do
    (0...MAX_JOB).each { |i|
      it "should get the correct fields for job #{i}" do
        job = raw_jobs[i]
        stub_api_request(:get, "/v3/job/show/#{e(i)}").to_return(:body => job.to_json)

        type, query, status, url, debug, start_at, end_at, cpu_time,
          result_size, result_url, hive_result_schema, priority, retry_limit, org, db = api.show_job(i)
        type.should == job['type']
        query.should == job['query']
        status.should == job['status']
        url.should == job['url']
        debug.should == job['debug']
        start_at.should == job['start_at']
        end_at.should == job['end_at']
        cpu_time.should == job['cpu_time']
        result_size.should == job['result_size']
        result_url.should == job['result_url']
        hive_result_schema.should == job['hive_result_schema']
        result_url.should == job['result_url']
        priority.should == job['priority']
        org.should == job['organization']
        db.should == job['database']
      end
    }

    it 'should return an error with unknown id' do
      unknown_id = 10000000000
      body = {"message" => "Couldn't find Job with account_id = #{account_id}, id = #{unknown_id}"}
      stub_api_request(:get, "/v3/job/show/#{e(unknown_id)}").to_return(:status => 404, :body => body.to_json)

      expect {
        api.show_job(unknown_id)
      }.to raise_error(TreasureData::NotFoundError, /Couldn't find Job with account_id = #{account_id}, id = #{unknown_id}/)
    end

    it 'should return an error with invalid id' do
      invalid_id = 'bomb!'
      body = {"message" => "'job_id' parameter is required but missing"}
      stub_api_request(:get, "/v3/job/show/#{e(invalid_id)}").to_return(:status => 500, :body => body.to_json)

      expect {
        api_with_short_retry.show_job(invalid_id)
      }.to raise_error(TreasureData::APIError, /'job_id' parameter is required but missing/)
    end
  end

  describe 'job status' do
    (0...MAX_JOB).each { |i|
      it "should return the status of a job #{i}" do
        job_id = i.to_s
        raw_job = raw_jobs[i]
        result_job = {
          'job_id' => raw_job['job_id'],
          'status' => raw_job['status'],
          'created_at' => raw_job['created_at'],
          'start_at' => raw_job['start_at'],
          'end_at' => raw_job['end_at'],
        }
        stub_api_request(:get, "/v3/job/status/#{e(job_id)}").to_return(:body => result_job.to_json)

        status = api.job_status(job_id)
        status.should == (i.odd? ? 'success' : 'error')
      end
    }
  end

  describe 'hive_query' do
    let :return_body do
      {:body => {'job_id' => '1'}.to_json}
    end

    it 'issue a query' do
      params =  {'query' => query}
      stub_api_request(:post, "/v3/job/issue/hive/#{e(db_name)}").with(:body => params).to_return(return_body)

      job_id = api.hive_query(query, db_name)
      job_id.should == '1'
    end

    it 'issue a query with result_url' do
      params =  {'query' => query, 'result' => 'td://@/test/table'}
      stub_api_request(:post, "/v3/job/issue/hive/#{e(db_name)}").with(:body => params).to_return(return_body)

      job_id = api.hive_query(query, db_name, 'td://@/test/table')
      job_id.should == '1'
    end

    it 'issue a query with priority' do
      params =  {'query' => query, 'priority' => '1'}
      stub_api_request(:post, "/v3/job/issue/hive/#{e(db_name)}").with(:body => params).to_return(return_body)

      job_id = api.hive_query(query, db_name, nil, 1)
      job_id.should == '1'
    end
  end
end
