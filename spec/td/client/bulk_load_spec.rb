require 'spec_helper'
require 'td/client/spec_resources'
require 'tempfile'

describe 'BulkImport API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    Client.new(nil, {:max_cumul_retry_delay => -1})
  end

  let :retry_api do
    API.new(nil, {:retry_delay => 1, :max_cumul_retry_delay => 1})
  end

  let :original_config do
    {
      :config => {
        :type => "s3_file",
        :access_key_id => "key",
        :secret_access_key => "secret",
        :endpoint => "s3.amazonaws.com",
        :bucket => "td-bulk-loader-test-tokyo",
        :path_prefix => "in/nahi/sample"
      }
    }
  end

  let :guessed_config do
    {
      "config" => {
        "type" => "s3_file",
        "access_key_id" => "key",
        "secret_access_key" => "secret",
        "endpoint" => "s3.amazonaws.com",
        "bucket" => "td-bulk-loader-test-tokyo",
        "path_prefix" => "in/nahi/sample",
        "parser" => {
          "charset" => "UTF-8",
          "newline" => "LF",
          "type" => "csv",
          "delimiter" => ",",
          "header_line" => false,
          "columns" => [
            {"name" => "time", "type" => "long"},
            {"name" => "c1", "type" => "long"},
            {"name" => "c2", "type" => "string"},
            {"name" => "c3", "type" => "string"},
          ]
        },
        "decoders" => [
          {"type" => "gzip"}
        ]
      }
    }
  end

  let :preview_result do
    {
      "schema" => [
        {"index" => 0, "name" => "c0", "type" => "string"},
        {"index" => 1, "name" => "c1", "type" => "long"},
        {"index" => 2, "name" => "c2", "type" => "string"},
        {"index" => 3, "name" => "c3", "type" => "string"}
      ],
      "records" => [
        ["19920116", 32864, "06612", "00195"],
        ["19910729", 14824, "07706", "00058"],
        ["19950708", 27559, "03244", "00034"],
        ["19931010", 11270, "03459", "00159"],
        ["19981117", 20461, "01409", "00128"],
        ["19981117", 20461, "00203", "00128"],
        ["19930108", 44402, "01489", "00001"],
        ["19960104", 16528, "04848", "00184"],
        ["19960104", 16528, "01766", "00184"],
        ["19881022", 26114, "06960", "00175"]
      ]
    }
  end

  let :bulk_load_session do
    guessed_config.dup.merge(
      {
        "name" => "nahi_test_1",
        "cron" => "@daily",
        "timezone" => "Asia/Tokyo",
        "delay" => 3600
      }
    )
  end

  let :bulk_load_job do
    guessed_config.dup.merge(
      {
        "job_id" => 123456,
        "account_id" => 1,
        "status" => "success",
        "records" => 10,
        "schema" => [["c0", "string", ""], ["c1", "long", ""], ["c2", "string", ""], ["c3", "string", ""]],
        "database" => {"id" => 189263, "name" => "nahidb"},
        "table" => {"id" => 176281, "name" => "bulkload_import_test"},
        "created_at" => 1426738133,
        "updated_at" => 1426738145,
        "start_at" => 1426738134,
        "end_at" => 1426738144
      }
    )
  end

  describe 'guess' do
    it 'returns guessed json' do
      stub_api_request(:post, '/v3/bulk_loads/guess').
        with(:body => original_config.to_json).
        to_return(:body => guessed_config.to_json)
      expect(api.bulk_load_guess(
        original_config
      )).to eq(guessed_config)
    end

    it 'raises an error' do
      stub_api_request(:post, '/v3/bulk_loads/guess').
        with(:body => original_config.to_json).
        to_return(:status => 500, :body => guessed_config.to_json)
      expect {
        api.bulk_load_guess(
          original_config
        )
      }.to raise_error(TreasureData::APIError)
    end

    it 'perform redo on 500 error' do
      stub_api_request(:post, '/v3/bulk_loads/guess').
        with(:body => original_config.to_json).
        to_return(:status => 500, :body => guessed_config.to_json)
      begin
        expect(retry_api.bulk_load_guess(
          original_config
        )).to != nil
      rescue TreasureData::APIError => e
        expect(e.message).to match(/^500: BulkLoad configuration guess failed/)
      end
    end

    it 'perform retries on connection failure' do
      api = retry_api
      allow(api.instance_eval { @api }).to receive(:post).and_raise(SocketError.new('>>'))
      begin
        retry_api.bulk_load_guess(
          original_config
        )
      rescue SocketError => e
        expect(e.message).to eq('>> (Retried 1 times in 1 seconds)')
      end
    end
  end

  describe 'guess with old format' do
    it 'returns guessed json' do
      stub_api_request(:post, '/v3/bulk_loads/guess').
        with(:body => original_config.to_json).
        to_return(:body => guessed_config.to_json)
      expect(api.bulk_load_guess(
        original_config
      )).to eq(guessed_config)
    end

    it 'raises an error' do
      stub_api_request(:post, '/v3/bulk_loads/guess').
        with(:body => original_config.to_json).
        to_return(:status => 500, :body => guessed_config.to_json)
      expect {
        api.bulk_load_guess(
          original_config
        )
      }.to raise_error(TreasureData::APIError)
    end

    it 'perform redo on 500 error' do
      stub_api_request(:post, '/v3/bulk_loads/guess').
        with(:body => original_config.to_json).
        to_return(:status => 500, :body => guessed_config.to_json)
      begin
        expect(retry_api.bulk_load_guess(
          original_config
        )).to != nil
      rescue TreasureData::APIError => e
        expect(e.message).to match(/^500: BulkLoad configuration guess failed/)
      end
    end

    it 'perform retries on connection failure' do
      api = retry_api
      allow(api.instance_eval { @api }).to receive(:post).and_raise(SocketError.new('>>'))
      begin
        retry_api.bulk_load_guess(
          original_config
        )
      rescue SocketError => e
        expect(e.message).to eq('>> (Retried 1 times in 1 seconds)')
      end
    end
  end

  describe 'preview' do
    it 'returns preview json' do
      stub_api_request(:post, '/v3/bulk_loads/preview').
        with(:body => guessed_config.to_json).
        to_return(:body => preview_result.to_json)
      expect(api.bulk_load_preview(
        guessed_config
      )).to eq(preview_result)
    end

    it 'raises an error' do
      stub_api_request(:post, '/v3/bulk_loads/preview').
        with(:body => guessed_config.to_json).
        to_return(:status => 500, :body => preview_result.to_json)
      expect {
        api.bulk_load_preview(
          guessed_config
        )
      }.to raise_error(TreasureData::APIError)
    end
  end

  describe 'issue' do
    it 'returns job id' do
      expected_request = guessed_config.dup
      expected_request['database'] = 'database'
      expected_request['table'] = 'table'
      stub_api_request(:post, '/v3/job/issue/bulkload/database').
        with(:body => expected_request.to_json).
        to_return(:body => {'job_id' => 12345}.to_json)
      expect(api.bulk_load_issue(
        'database',
        'table',
        guessed_config
      )).to eq('12345')
    end
  end

  describe 'list' do
    it 'returns BulkLoadSession' do
      stub_api_request(:get, '/v3/bulk_loads').
        to_return(:body => [bulk_load_session, bulk_load_session].to_json)
      result = api.bulk_load_list
      expect(result.size).to eq(2)
      expect(result.first).to eq(bulk_load_session)
    end

    it 'returns empty' do
      stub_api_request(:get, '/v3/bulk_loads').
        to_return(:body => [].to_json)
      expect(api.bulk_load_list.size).to eq(0)
    end
  end

  describe 'create' do
    it 'returns registered bulk_load_session' do
      expected_request = guessed_config.dup
      expected_request['name'] = 'nahi_test_1'
      expected_request['cron'] = '@daily'
      expected_request['timezone'] = 'Asia/Tokyo'
      expected_request['delay'] = 3600
      expected_request['database'] = 'database'
      expected_request['table'] = 'table'
      stub_api_request(:post, '/v3/bulk_loads').
        with(:body => expected_request.to_json).
        to_return(:body => bulk_load_session.to_json)
      expect(api.bulk_load_create(
        'nahi_test_1',
        'database',
        'table',
        guessed_config,
        {
          cron: '@daily',
          timezone: 'Asia/Tokyo',
          delay: 3600
        }
      )).to eq(bulk_load_session)
    end

    it 'accepts empty option' do
      expected_request = guessed_config.dup
      expected_request['name'] = 'nahi_test_1'
      expected_request['database'] = 'database'
      expected_request['table'] = 'table'
      stub_api_request(:post, '/v3/bulk_loads').
        with(:body => expected_request.to_json).
        to_return(:body => bulk_load_session.to_json)
      expect(api.bulk_load_create(
        'nahi_test_1',
        'database',
        'table',
        guessed_config
      )).to eq(bulk_load_session)
    end

    it 'accepts time_column option' do
      expected_request = guessed_config.dup
      expected_request['name'] = 'nahi_test_1'
      expected_request['time_column'] = 'c0'
      expected_request['database'] = 'database'
      expected_request['table'] = 'table'
      stub_api_request(:post, '/v3/bulk_loads').
        with(:body => expected_request.to_json).
        to_return(:body => bulk_load_session.to_json)
      expect(api.bulk_load_create(
        'nahi_test_1',
        'database',
        'table',
        guessed_config,
        {
          time_column: 'c0'
        }
      )).to eq(bulk_load_session)
    end
  end

  describe 'show' do
    it 'returns bulk_load_session' do
      stub_api_request(:get, '/v3/bulk_loads/nahi_test_1').
        to_return(:body => bulk_load_session.to_json)
      expect(api.bulk_load_show('nahi_test_1')).to eq(bulk_load_session)
    end
  end

  describe 'update' do
    it 'returns updated bulk_load_session' do
      stub_api_request(:put, '/v3/bulk_loads/nahi_test_1').
        with(:body => bulk_load_session.to_json).
        to_return(:body => bulk_load_session.to_json)
      expect(api.bulk_load_update(
        'nahi_test_1',
        bulk_load_session
      )).to eq(bulk_load_session)
    end
  end

  describe 'delete' do
    it 'returns updated bulk_load_session' do
      stub_api_request(:delete, '/v3/bulk_loads/nahi_test_1').
        to_return(:body => bulk_load_session.to_json)
      expect(api.bulk_load_delete('nahi_test_1')).to eq(bulk_load_session)
    end
  end

  describe 'history' do
    it 'returns list of jobs' do
      stub_api_request(:get, '/v3/bulk_loads/nahi_test_1/jobs').
        to_return(:body => [bulk_load_job, bulk_load_job].to_json)
      result = api.bulk_load_history('nahi_test_1')
      expect(result.size).to eq(2)
      expect(result.first).to eq(bulk_load_job)
    end
  end

  describe 'run' do
    it 'returns job_id' do
      stub_api_request(:post, '/v3/bulk_loads/nahi_test_1/jobs').
        with(:body => '{}').
        to_return(:body => {'job_id' => 12345}.to_json)
      expect(api.bulk_load_run('nahi_test_1')).to eq('12345')
    end

    it 'accepts scheduled_time' do
      now = Time.now.to_i
      stub_api_request(:post, '/v3/bulk_loads/nahi_test_1/jobs').
        with(:body => {scheduled_time: now.to_s}.to_json).
        to_return(:body => {'job_id' => 12345}.to_json)
      expect(api.bulk_load_run('nahi_test_1', now)).to eq('12345')
    end
  end

end
