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
      expect(jobs.size).to eq(20)
    end

    (0...MAX_JOB).each {|i|
      it "should get the correct fields for job #{i} of 20" do
        job = raw_jobs[i]
        stub_api_request(:get, "/v3/job/list", :query => {'from' => '0'}).to_return(:body => {'jobs' => raw_jobs}.to_json)

        jobs = api.list_jobs
        jobs[i..i].map {|job_id, type, status, query, start_at, end_at, cpu_time,
                      result_size, result_url, priority, retry_limit, org, db,
                      duration, num_records|
          expect(job_id).to eq(job['job_id'])
          expect(type).to eq(job['type'])
          expect(status).to eq(job['status'])
          expect(query).to eq(job['query'])
          expect(start_at).to eq(job['start_at'])
          expect(end_at).to eq(job['end_at'])
          expect(cpu_time).to eq(job['cpu_time'])
          expect(result_size).to eq(job['result_size'])
          expect(result_url).to eq(job['result_url'])
          expect(priority).to eq(job['priority'])
          expect(retry_limit).to eq(job['retry_limit'])
          expect(org).to eq(job['organization'])
          expect(db).to eq(job['database'])
          expect(duration).to eq(job['duration'])
          expect(num_records).to eq(job['num_records'])
        }
      end
    }

    it 'should returns 10 jobs with to parameter' do
      stub_api_request(:get, "/v3/job/list", :query => {'from' => '0', 'to' => '10'}).to_return(:body => {'jobs' => raw_jobs[0...10]}.to_json)
      jobs = api.list_jobs(0, 10)
      expect(jobs.size).to eq(10)
    end

    it 'should returns 10 jobs with to status parameter' do
      error_jobs = raw_jobs.select { |j| j['status'] == 'error' }
      stub_api_request(:get, "/v3/job/list", :query => {'from' => '0', 'status' => 'error'}).to_return(:body => {'jobs' => error_jobs}.to_json)
      jobs = api.list_jobs(0, nil, 'error')
      expect(jobs.size).to eq(error_jobs.size)
    end

    #it 'should contain the result_size field' do

  end

  describe 'show_job' do
    (0...MAX_JOB).each { |i|
      it "should get the correct fields for job #{i}" do
        job = raw_jobs[i]
        stub_api_request(:get, "/v3/job/show/#{e(i)}").to_return(:body => job.to_json)

        type, query, status, url, debug, start_at, end_at, cpu_time,
          result_size, result_url, hive_result_schema, priority, retry_limit, org, db, num_records = api.show_job(i)
        expect(type).to eq(job['type'])
        expect(query).to eq(job['query'])
        expect(status).to eq(job['status'])
        expect(url).to eq(job['url'])
        expect(debug).to eq(job['debug'])
        expect(start_at).to eq(job['start_at'])
        expect(end_at).to eq(job['end_at'])
        expect(cpu_time).to eq(job['cpu_time'])
        expect(result_size).to eq(job['result_size'])
        expect(result_url).to eq(job['result_url'])
        expect(hive_result_schema).to eq(job['hive_result_schema'])
        expect(result_url).to eq(job['result_url'])
        expect(priority).to eq(job['priority'])
        expect(org).to eq(job['organization'])
        expect(db).to eq(job['database'])
        expect(num_records).to eq(job['num_records'])
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
        expect(status).to eq(i.odd? ? 'success' : 'error')
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
      expect(job_id).to eq('1')
    end

    it 'issue a query with result_url' do
      params =  {'query' => query, 'result' => 'td://@/test/table'}
      stub_api_request(:post, "/v3/job/issue/hive/#{e(db_name)}").with(:body => params).to_return(return_body)

      job_id = api.hive_query(query, db_name, 'td://@/test/table')
      expect(job_id).to eq('1')
    end

    it 'issue a query with priority' do
      params =  {'query' => query, 'priority' => '1'}
      stub_api_request(:post, "/v3/job/issue/hive/#{e(db_name)}").with(:body => params).to_return(return_body)

      job_id = api.hive_query(query, db_name, nil, 1)
      expect(job_id).to eq('1')
    end
  end

  describe 'job_result' do
    let :packed do
      s = StringIO.new(String.new)
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
      expect(api.job_result(12345)).to eq(['hello', 'world'])
    end

    it '200->200 cannot resume' do
      sz = packed.bytesize / 3
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'msgpack'}).
        to_return(
          :headers => {
            'Content-Length' => packed.bytesize,
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[0, sz]
        )
      stub_api_request(:get, '/v3/job/result/12345').
        with(
          :headers => {
            'If-Range' => '"abcdefghijklmn"',
            'Range' => "bytes=#{sz}-",
          },
          :query => {'format' => 'msgpack'}
        ).
        to_return(
          :status => 200,
          :headers => {
            'Content-Length' => packed.bytesize - sz,
            'Content-Range' => "bytes #{sz}-#{packed.bytesize-1}/#{packed.bytesize}",
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed
        )
      expect(api).to receive(:sleep).once
      expect($stderr).to receive(:print)
      expect($stderr).to receive(:puts)
      expect{api.job_result(12345)}.to raise_error(TreasureData::APIError)
    end

    it '200->403 cannot resume' do
      sz = packed.bytesize / 3
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'msgpack'}).
        to_return(
          :headers => {
            'Content-Length' => packed.bytesize,
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[0, sz]
        )
      stub_api_request(:get, '/v3/job/result/12345').
        with(
          :headers => {
            'If-Range' => '"abcdefghijklmn"',
            'Range' => "bytes=#{sz}-",
          },
          :query => {'format' => 'msgpack'}
        ).
        to_return(
          :status => 403,
          :headers => {
            'Content-Length' => packed.bytesize - sz,
            'Content-Range' => "bytes #{sz}-#{packed.bytesize-1}/#{packed.bytesize}",
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed
        )
      expect(api).to receive(:sleep).once
      expect($stderr).to receive(:print)
      expect($stderr).to receive(:puts)
      expect{api.job_result(12345)}.to raise_error(TreasureData::APIError)
    end

    it 'can resume' do
      sz = packed.bytesize / 3
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'msgpack'}).
        to_return(
          :headers => {
            'Content-Length' => packed.bytesize,
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[0, sz]
        )
      stub_api_request(:get, '/v3/job/result/12345').
        with(
          :headers => {
            'If-Range' => '"abcdefghijklmn"',
            'Range' => "bytes=#{sz}-",
          },
          :query => {'format' => 'msgpack'}
        ).
        to_return(
          :status => 206,
          :headers => {
            'Content-Length' => packed.bytesize - sz,
            'Content-Range' => "bytes #{sz}-#{packed.bytesize-1}/#{packed.bytesize}",
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[sz, packed.bytesize - sz]
        )
      expect(api).to receive(:sleep).once
      expect($stderr).to receive(:print)
      expect($stderr).to receive(:puts)
      expect(api.job_result(12345)).to eq ['hello', 'world']
    end

    it '200->500->206 can resume' do
      sz = packed.bytesize / 3
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'msgpack'}).
        to_return(
          :headers => {
            'Content-Length' => packed.bytesize,
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[0, sz]
        )
      stub_api_request(:get, '/v3/job/result/12345').
        with(
          :headers => {
            'If-Range' => '"abcdefghijklmn"',
            'Range' => "bytes=#{sz}-",
          },
          :query => {'format' => 'msgpack'}
        ).
        to_return(
          :status => 500,
          :headers => {
            'Content-Length' => packed.bytesize - sz,
            'Content-Range' => "bytes #{sz}-#{packed.bytesize-1}/#{packed.bytesize}",
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed
        )
      stub_api_request(:get, '/v3/job/result/12345').
        with(
          :headers => {
            'If-Range' => '"abcdefghijklmn"',
            'Range' => "bytes=#{sz}-",
          },
          :query => {'format' => 'msgpack'}
        ).
        to_return(
          :status => 206,
          :headers => {
            'Content-Length' => packed.bytesize - sz,
            'Content-Range' => "bytes #{sz}-#{packed.bytesize-1}/#{packed.bytesize}",
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[sz, packed.bytesize - sz]
        )
      expect(api).to receive(:sleep).once
      expect($stderr).to receive(:print)
      expect($stderr).to receive(:puts)
      expect(api.job_result(12345)).to eq ['hello', 'world']
    end

  end

  describe 'job_result_format' do
    let :packed do
      s = StringIO.new(String.new)
      Zlib::GzipWriter.wrap(s) do |f|
        f << ['hello', 'world'].to_json
      end
      s.string
    end

    context 'Content-Encoding is empty' do
      let(:io) { StringIO.new }
      let(:json) { ['hello', 'world'].to_json }

      it 'retrunes json' do
        stub_api_request(:get, '/v3/job/result/12345').
          with(:query => {'format' => 'json'}).
          to_return(:body => json)

        total_size = 0
        api.job_result_format(12345, 'json', io) {|size| total_size += size }

        expect(io.string).to  eq(json)
        expect(total_size).to eq(json.size)
      end
    end

    context 'Content-Encoding is gzip' do
      it 'returns formatted job result' do
        stub_api_request(:get, '/v3/job/result/12345').
          with(:query => {'format' => 'json'}).
          to_return(
            :headers => {'Content-Encoding' => 'gzip'},
            :body => packed
          )
        expect(api.job_result_format(12345, 'json')).to eq(['hello', 'world'].to_json)
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
        expect(s.string).to eq(['hello', 'world'].to_json)
      end

      context 'can resume' do
        before do
          sz = packed.bytesize / 3
          stub_api_request(:get, '/v3/job/result/12345').
            with(:query => {'format' => 'json'}).
            to_return(
              :headers => {
                'Content-Encoding' => 'gzip',
                'Content-Length' => packed.bytesize,
                'Etag' => '"abcdefghijklmn"',
              },
              :body => packed[0, sz]
            )
          stub_api_request(:get, '/v3/job/result/12345').
            with(
              :headers => {
                'If-Range' => '"abcdefghijklmn"',
                'Range' => "bytes=#{sz}-",
              },
              :query => {'format' => 'json'}
            ).
          to_return(
            :status => 206,
            :headers => {
              'Content-Encoding' => 'gzip',
              'Content-Length' => packed.bytesize-sz,
              'Content-Range' => "bytes #{sz}-#{packed.bytesize-1}/#{packed.bytesize}",
              'Etag' => '"abcdefghijklmn"',
            },
            :body => packed[sz, packed.bytesize-sz]
          )
          expect(api).to receive(:sleep).once
          expect($stderr).to receive(:print)
          expect($stderr).to receive(:puts)
        end
        it 'can work with io' do
          s = StringIO.new
          api.job_result_format(12345, 'json', s)
          expect(s.string).to eq ['hello', 'world'].to_json
        end
        it 'can work without block' do
          expect(api.job_result_format(12345, 'json')).to eq ['hello', 'world'].to_json
        end
      end
    end
  end

  describe 'job_result_each' do
    let :packed do
      s = StringIO.new(String.new)
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
      expect(result).to eq(['hello', 'world'])
    end

    it 'can resume' do
      sz= packed.bytesize / 3
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'msgpack'}).
        to_return(
          :headers => {
            'Content-Encoding' => 'gzip',
            'Content-Length' => packed.bytesize,
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[0, sz]
        )
      stub_api_request(:get, '/v3/job/result/12345').
        with(
          :headers => {
            'If-Range' => '"abcdefghijklmn"',
            'Range' => "bytes=#{sz}-",
          },
          :query => {'format' => 'msgpack'}
        ).
        to_return(
          :status => 206,
          :headers => {
            'Content-Length' => packed.bytesize-sz,
            'Content-Range' => "bytes #{sz}-#{packed.bytesize-1}/#{packed.bytesize}",
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[sz, packed.bytesize-sz]
        )
      expect(api).to receive(:sleep).once
      expect($stderr).to receive(:print)
      expect($stderr).to receive(:puts)
      result = []
      api.job_result_each(12345) do |row|
        result << row
      end
      expect(result).to eq ['hello', 'world']
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
      "\x1F\x8B\b\x00#\xA1\x93T\x00\x03[\x9A\x91\x9A\x93\x93\xBF\xB4<\xBF('\x05\x00e 0\xB3\f\x00\x00\x00".force_encoding(Encoding::ASCII_8BIT)
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
      expect(result).to eq([['hello', 32], ['world', 32]])
    end

    it 'can resume' do
      sz = packed.bytesize / 3
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'msgpack'}).
        to_return(
          :headers => {
            'Content-Encoding' => 'gzip',
            'Content-Length' => packed.bytesize,
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[0, sz]
        )
      stub_api_request(:get, '/v3/job/result/12345').
        with(
          :headers => {
            'If-Range' => '"abcdefghijklmn"',
            'Range' => "bytes=#{sz}-",
          },
          :query => {'format' => 'msgpack'}
        ).
        to_return(
          :status => 206,
          :headers => {
            'Content-Length' => packed.bytesize - sz,
            'Content-Range' => "bytes #{sz}-#{packed.bytesize-1}/#{packed.bytesize}",
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[sz, packed.bytesize - sz]
        )
      expect(api).to receive(:sleep).once
      expect($stderr).to receive(:print)
      expect($stderr).to receive(:puts)
      result = []
      api.job_result_each_with_compr_size(12345) do |row, size|
        result << [row, size]
      end
      expect(result).to eq [['hello', 32], ['world', 32]]
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

        expect(io.string).to eq('raw binary')
      end
    end

    context 'witout io' do
      it 'returns raw result' do
        stub_api_request(:get, '/v3/job/result/12345').
          with(:query => {'format' => 'json'}).
          to_return(:body => 'raw binary')
        expect(api.job_result_raw(12345, 'json')).to eq('raw binary')
      end
    end

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
      "\x1F\x8B\b\x00#\xA1\x93T\x00\x03[\x9A\x91\x9A\x93\x93\xBF\xB4<\xBF('\x05\x00e 0\xB3\f\x00\x00\x00".force_encoding(Encoding::ASCII_8BIT)
    end

    it 'can resume' do
      sz = packed.bytesize / 3
      stub_api_request(:get, '/v3/job/result/12345').
        with(:query => {'format' => 'msgpack.gz'}).
        to_return(
          :headers => {
            'Content-Length' => packed.bytesize,
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[0, sz]
        )
      stub_api_request(:get, '/v3/job/result/12345').
        with(
          :headers => {
            'If-Range' => '"abcdefghijklmn"',
            'Range' => "bytes=#{sz}-",
          },
          :query => {'format' => 'msgpack.gz'}
        ).
        to_return(
          :status => 206,
          :headers => {
            'Content-Length' => packed.bytesize - sz,
            'Content-Range' => "bytes #{sz}-#{packed.bytesize-1}/#{packed.bytesize}",
            'Etag' => '"abcdefghijklmn"',
          },
          :body => packed[sz, packed.bytesize - sz]
        )
      expect(api).to receive(:sleep).once
      expect($stderr).to receive(:print)
      expect($stderr).to receive(:puts)
      sio = StringIO.new(String.new)
      api.job_result_raw(12345, 'msgpack.gz', sio)
      expect(sio.string).to eq(packed)
    end
  end

  describe 'kill' do
    it 'kills a job' do
      stub_api_request(:post, '/v3/job/kill/12345').
        to_return(:body => {'former_status' => 'status'}.to_json)
      expect(api.kill(12345)).to eq('status')
    end
  end

  describe 'job_result_download' do
    let (:data){ [[1, 'hello', nil], [2, 'world', true], [3, '!', false]] }
    let :formatted do
      case format
      when 'json'
        data.map{|a| JSON(a) }.join("\n")
      when 'msgpack'
        pk = MessagePack::Packer.new
        data.each{|x| pk.write(x) }
        pk.to_str
      else
        raise
      end
    end
    let :gziped do
      s = StringIO.new(String.new)
      Zlib::GzipWriter.wrap(s) do |f|
        f.write formatted
      end
      s.string
    end
    let :deflated do
      Zlib::Deflate.deflate(formatted)
    end
    subject do
      str = ''
      api.__send__(:job_result_download, 12345, format){|x| str << x }
      str
    end
    context '200' do
      before do
        sz = packed.bytesize / 3
        stub_api_request(:get, '/v3/job/result/12345').
          with(:query => {'format' => format}).
          to_return(
            :headers => {
              'Content-Encoding' => content_encoding,
              'Content-Length' => packed.bytesize,
              'Etag' => '"abcdefghijklmn"',
            },
            :body => packed
          )
        expect(api).not_to receive(:sleep)
        expect($stderr).not_to receive(:print)
        expect($stderr).not_to receive(:puts)
      end
      context 'Content-Encoding: gzip' do
        let (:content_encoding){ 'gzip' }
        let (:packed){ gziped }
        context 'msgpack' do
          let (:format){ 'msgpack' }
          it { is_expected.to eq formatted }
        end
        context 'json' do
          let (:format){ 'json' }
          it { is_expected.to eq formatted }
        end
      end
      context 'Content-Encoding: deflate' do
        let (:content_encoding){ 'deflate' }
        let (:packed){ deflated }
        context 'msgpack' do
          let (:format){ 'msgpack' }
          it { is_expected.to eq formatted }
        end
        context 'json' do
          let (:format){ 'json' }
          it { is_expected.to eq formatted }
        end
      end
    end

    context '200 -> 206' do
      before do
        sz = packed.bytesize / 3
        stub_api_request(:get, '/v3/job/result/12345').
          with(:query => {'format' => format}).
          to_return(
            :headers => {
              'Content-Encoding' => content_encoding,
              'Content-Length' => packed.bytesize,
              'Etag' => '"abcdefghijklmn"',
            },
            :body => packed[0, sz]
        )
        stub_api_request(:get, '/v3/job/result/12345').
          with(
            :headers => {
              'If-Range' => '"abcdefghijklmn"',
              'Range' => "bytes=#{sz}-",
          },
          :query => {'format' => format}
        ).
          to_return(
            :status => 206,
            :headers => {
              'Content-Encoding' => content_encoding,
              'Content-Length' => packed.bytesize - sz,
              'Content-Range' => "bytes #{sz}-#{packed.bytesize-1}/#{packed.bytesize}",
              'Etag' => '"abcdefghijklmn"',
            },
            :body => packed[sz, packed.bytesize - sz]
        )
        expect(api).to receive(:sleep).once
        allow($stderr).to receive(:print)
        allow($stderr).to receive(:puts)
      end
      context 'Content-Encoding: gzip' do
        let (:content_encoding){ 'gzip' }
        let (:packed){ gziped }
        context 'msgpack' do
          let (:format){ 'msgpack' }
          it { is_expected.to eq formatted }
        end
        context 'json' do
          let (:format){ 'json' }
          it { is_expected.to eq formatted }
        end
      end
      context 'Content-Encoding: deflate' do
        let (:content_encoding){ 'deflate' }
        let (:packed){ deflated }
        context 'msgpack' do
          let (:format){ 'msgpack' }
          it { is_expected.to eq formatted }
        end
        context 'json' do
          let (:format){ 'json' }
          it { is_expected.to eq formatted }
        end
      end
    end

    context 'without autodecode' do
      before do
        sz = packed.bytesize / 3
        stub_api_request(:get, '/v3/job/result/12345').
          with(:query => {'format' => format}).
          to_return(
            :headers => {
              'Content-Length' => packed.bytesize,
              'Etag' => '"abcdefghijklmn"',
            },
            :body => packed
          )
        expect(api).not_to receive(:sleep)
        expect($stderr).not_to receive(:print)
        expect($stderr).not_to receive(:puts)
      end
      subject do
        str = ''
        api.__send__(:job_result_download, 12345, format, false){|x| str << x }
        str
      end
      context 'Content-Encoding: gzip' do
        let (:content_encoding){ 'gzip' }
        let (:packed){ gziped }
        context 'msgpack' do
          let (:format){ 'msgpack' }
          it { is_expected.to eq packed }
        end
        context 'json' do
          let (:format){ 'json' }
          it { is_expected.to eq packed }
        end
      end
      context 'Content-Encoding: deflate' do
        let (:content_encoding){ 'deflate' }
        let (:packed){ deflated }
        context 'msgpack' do
          let (:format){ 'msgpack' }
          it { is_expected.to eq packed }
        end
        context 'json' do
          let (:format){ 'json' }
          it { is_expected.to eq packed }
        end
      end
    end
  end
end
