require 'spec_helper'
require 'td/client/spec_resources'
require 'json'
require 'webrick'
require 'logger'

describe 'Job API' do
  include_context 'spec symbols'
  include_context 'job resources'

  let :api do
    API.new(nil)
  end

  let :api_with_short_retry do
    API.new(nil, {:max_cumul_retry_delay => 0})
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
                      result_size, result_url, priority, retry_limit, org, db,
                      duration|
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
          duration.should == job['duration']
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

  describe 'job_result' do
    let :packed do
      s = StringIO.new
      pk = MessagePack::Packer.new(s)
      pk.write('hello')
      pk.write('world')
      pk.flush
      s.string
    end

    it 'returns job result' do
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'msgpack'}).
        to_return(:body => packed)
      api.job_result(12345).should == ['hello', 'world']
    end
  end

  describe 'job_result_format' do
    let :packed do
      s = StringIO.new
      Zlib::GzipWriter.wrap(s) do |f|
        f << ['hello', 'world'].to_json
      end
      s.string
    end

    it 'returns formatted job result' do
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'json'}).
        to_return(
          :headers => {'Content-Encoding' => 'gzip'},
          :body => packed
        )
      api.job_result_format(12345, 'json').should == ['hello', 'world'].to_json
    end

    it 'writes formatted job result' do
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'json'}).
        to_return(
          :headers => {'Content-Encoding' => 'gzip'},
          :body => packed
        )
      s = StringIO.new
      api.job_result_format(12345, 'json', s)
      s.string.should == ['hello', 'world'].to_json
    end
  end

  describe 'job_result_each' do
    let :packed do
      s = StringIO.new
      Zlib::GzipWriter.wrap(s) do |f|
        pk = MessagePack::Packer.new(f)
        pk.write('hello')
        pk.write('world')
        pk.flush
      end
      s.string
    end

    it 'yields job result for each row' do
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'msgpack'}).
        to_return(
          :headers => {'Content-Encoding' => 'gzip'},
          :body => packed
        )
      result = []
      api.job_result_each(12345) do |row|
        result << row
      end
      result.should == ['hello', 'world']
    end
  end

  describe 'job_result_each_with_compr_size' do
    let :packed do
      # Hard code fixture data to make the size stable
      # s = StringIO.new
      # Zlib::GzipWriter.wrap(s) do |f|
      #   pk = MessagePack::Packer.new(f)
      #   pk.write('hello')
      #   pk.write('world')
      #   pk.flush
      # end
      # s.string
      "\u001F\x8B\b\u0000#\xA1\x93T\u0000\u0003[\x9A\x91\x9A\x93\x93\xBF\xB4<\xBF('\u0005\u0000e 0\xB3\f\u0000\u0000\u0000"
    end

    it 'yields job result for each row with progress' do
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'msgpack'}).
        to_return(
          :headers => {'Content-Encoding' => 'gzip'},
          :body => packed
        )
      result = []
      api.job_result_each_with_compr_size(12345) do |row, size|
        result << [row, size]
      end
      result.should == [['hello', 32], ['world', 32]]
    end
  end

  describe 'job_result_raw' do
    context 'with io' do
      let(:io) { StringIO.new }

      it 'returns raw result' do
        stub_api_request(:get, '/v3/job/result/12345').
          with(:query => {'format' => 'json'}).
          to_return(:body => 'raw binary')
        api.job_result_raw(12345, 'json', io)

        io.string.should == 'raw binary'
      end
    end

    context 'witout io' do
      it 'returns raw result' do
        stub_api_request(:get, '/v3/job/result/12345').
          with(:query => {'format' => 'json'}).
          to_return(:body => 'raw binary')
        api.job_result_raw(12345, 'json').should == 'raw binary'
      end
    end
  end

  describe 'kill' do
    it 'kills a job' do
      stub_api_request(:post, '/v3/job/kill/12345').
        to_return(:body => {'former_status' => 'status'}.to_json)
      api.kill(12345).should == 'status'
    end
  end
end
