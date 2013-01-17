require 'spec_helper'
require 'td/client/spec_resources'

describe 'Job API' do
  include_context 'job resources'

  let :api do
    API.new(nil)
  end

  describe 'list_jobs' do
    it 'should returns 20 jobs by default' do
      stub_api_request(:get, "/v3/job/list", :query => {'from' => '0'}).to_return(:body => {'jobs' => raw_jobs}.to_json)
      jobs = api.list_jobs
      jobs.size.should == 20
    end

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
  end

  describe 'show_job' do
    (0...MAX_JOB).each { |i|
      it "should get a job of id #{i}" do
        job = raw_jobs[i]
        stub_api_request(:get, "/v3/job/show/#{e(i)}").to_return(:body => job.to_json)

        type, query, status, url, debug, start_at, end_at, result_url, hive_result_schema, priority, retry_limit, org, db = api.show_job(i)
        type.should == job['type']
        query.should == job['query']
        status.should == job['status']
        url.should == job['url']
        debug.should == job['debug']
        start_at.should == job['start_at']
        end_at.should == job['end_at']
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
      }.to raise_error(TreasureData::NotFoundError, /^Couldn't find Job with account_id = #{account_id}, id = #{unknown_id}/)
    end

    it 'should return an error with invalid id' do
      invalid_id = 'bomb!'
      body = {"message" => "'job_id' parameter is required but missing"}
      stub_api_request(:get, "/v3/job/show/#{e(invalid_id)}").to_return(:status => 500, :body => body.to_json)

      expect {
        api.show_job(invalid_id)
      }.to raise_error(TreasureData::APIError, /^'job_id' parameter is required but missing/)
    end
  end

  describe 'hive_query' do
    let :db do
      'test'
    end

    let :query do
      'select 1'
    end

    let :return_body do
      {:body => {'job_id' => '1'}.to_json}
    end

    it 'issue a query' do
      params =  {'query' => query}
      stub_api_request(:post, "/v3/job/issue/hive/#{e(db)}").with(:body => params).to_return(return_body)

      job_id = api.hive_query(query, db)
      job_id.should == '1'
    end

    it 'issue a query with result_url' do
      params =  {'query' => query, 'result' => 'td://@/test/table'}
      stub_api_request(:post, "/v3/job/issue/hive/#{e(db)}").with(:body => params).to_return(return_body)

      job_id = api.hive_query(query, db, 'td://@/test/table')
      job_id.should == '1'
    end

    it 'issue a query with priority' do
      params =  {'query' => query, 'priority' => '1'}
      stub_api_request(:post, "/v3/job/issue/hive/#{e(db)}").with(:body => params).to_return(return_body)

      job_id = api.hive_query(query, db, nil, 1)
      job_id.should == '1'
    end
  end
end
