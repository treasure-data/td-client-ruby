require 'spec_helper'
require 'td/client/spec_resources'
require 'json'
require 'tempfile'

describe 'Import API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil, :endpoint => endpoint)
  end

  let :api_old do
    API.new(nil, :endpoint => endpoint_old)
  end

  let(:endpoint) { "https://#{TreasureData::API::DEFAULT_ENDPOINT}" }
  let(:endpoint_old) { 'http://api.treasure-data.com' }
  let(:endpoint_import) { "https://#{TreasureData::API::DEFAULT_IMPORT_ENDPOINT}" }
  let(:endpoint_import_old) { "http://api-import.treasure-data.com" }

  describe 'import' do
    it 'runs with unique_id' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "#{endpoint_import}/v3/table/import_with_id/db/table/unique_id/format").
        with(:body => '12345').
        to_return(:body => '{"elapsed_time":"1.23"}')
      File.open(t.path) do |f|
        expect(api.import('db', 'table', 'format', f, 5, 'unique_id')).to eq(1.23)
      end
    end

    it 'runs without unique_id' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "#{endpoint_import}/v3/table/import/db/table/format").
        with(:body => '12345').
        to_return(:body => '{"elapsed_time":"1.23"}')
      File.open(t.path) do |f|
        expect(api.import('db', 'table', 'format', f, 5)).to eq(1.23)
      end
    end

    it 'runs for old endpoint' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "#{endpoint_import_old}/v3/table/import/db/table/format").
        with(:body => '12345').
        to_return(:body => '{"elapsed_time":"1.23"}')
      File.open(t.path) do |f|
        expect(api_old.import('db', 'table', 'format', f, 5)).to eq(1.23)
      end
    end

    it 'raises APIError' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "#{endpoint_import}/v3/table/import/db/table/format").
        with(:body => '12345').
        to_return(:status => 500)
      File.open(t.path) do |f|
        expect {
          api.import('db', 'table', 'format', f, 5)
        }.to raise_error(TreasureData::APIError)
      end
    end
  end
end
