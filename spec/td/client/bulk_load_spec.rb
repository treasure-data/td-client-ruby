require 'spec_helper'
require 'td/client/spec_resources'
require 'tempfile'

describe 'BulkImport API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil, {:max_cumul_retry_delay => -1})
  end

  let :original_config do
    {
      "config" => {
        "in" => {
          "type" => "s3_file",
          "access_key_id" => "key",
          "secret_access_key" => "secret",
          "endpoint" => "s3.amazonaws.com",
          "bucket" => "td-bulk-loader-test-tokyo",
          "paths" => [
            "in/nahi/sample"
          ],
        },
        "out" => {
          "type" => "td",
          "unique_transaction_name" => "bulk_load_session.1",
          "td_apikey" => "1/2345",
        }
      }
    }
  end

  let :guessed_config do
    {
      "config" => {
        "in" => {
          "type" => "s3_file",
          "access_key_id" => "key",
          "secret_access_key" => "secret",
          "endpoint" => "s3.amazonaws.com",
          "bucket" => "td-bulk-loader-test-tokyo",
          "paths" => [
            "in/nahi/sample"
          ],
          "parser" => {
            "file_decoders" => [
              {"type" => "gzip"}
            ],
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
          }
        },
        "out" => {
          "type" => "td",
          "unique_transaction_name" => "bulk_load_session.1",
          "td_apikey" => "1/2345",
        }
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

  describe 'guess' do
    it 'returns guessed json' do
      stub_api_request(:post, '/v3/bulk_loads/guess').
        with(:body => {'guess' => original_config.to_json}).
        to_return(:body => guessed_config.to_json)
      api.bulk_load_guess(original_config).should == guessed_config
    end

    it 'raises an error' do
      stub_api_request(:post, '/v3/bulk_loads/guess').
        with(:body => {'guess' => original_config.to_json}).
        to_return(:status => 500, :body => guessed_config.to_json)
      expect {
        api.bulk_load_guess(original_config)
      }.to raise_error(TreasureData::APIError)
    end
  end

  describe 'preview' do
    it 'returns preview json' do
      stub_api_request(:post, '/v3/bulk_loads/preview').
        with(:body => {'preview' => guessed_config.to_json}).
        to_return(:body => preview_result.to_json)
      api.bulk_load_preview(guessed_config).should == preview_result
    end

    it 'raises an error' do
      stub_api_request(:post, '/v3/bulk_loads/preview').
        with(:body => {'preview' => guessed_config.to_json}).
        to_return(:status => 500, :body => preview_result.to_json)
      expect {
        api.bulk_load_preview(guessed_config)
      }.to raise_error(TreasureData::APIError)
    end
  end
end
