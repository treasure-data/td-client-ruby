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

  let :api_default do
    API.new(nil)
  end

  let :api_default_http do
    API.new(nil, :ssl => false)
  end

  let :api_unknown_host do
    API.new(nil, :endpoint => endpoint_unknown)
  end

  let :api_unknown_host_http do
    API.new(nil, :endpoint => endpoint_unknown, :ssl => false)
  end

  let(:endpoint) { 'api.treasuredata.com' }
  let(:endpoint_old) { 'api.treasure-data.com' }
  let(:endpoint_unknown) { "example.com" }
  let(:endpoint_import) { "api-import.treasuredata.com" }
  let(:endpoint_import_old) { "api-import.treasure-data.com" }
  let(:endpoint_import_unknown) { endpoint_unknown }

  describe 'import' do
    it 'runs with unique_id' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "https://#{endpoint_import}/v3/table/import_with_id/db/table/unique_id/format").
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
      stub_request(:put, "https://#{endpoint_import}/v3/table/import/db/table/format").
        with(:body => '12345').
        to_return(:body => '{"elapsed_time":"1.23"}')
      File.open(t.path) do |f|
        expect(api.import('db', 'table', 'format', f, 5)).to eq(1.23)
      end
    end

    it 'runs for old endpoint (force "http" instead of "https" for compatibility)' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "http://#{endpoint_import_old}/v3/table/import/db/table/format").
        with(:body => '12345').
        to_return(:body => '{"elapsed_time":"1.23"}')
      File.open(t.path) do |f|
        expect(api_old.import('db', 'table', 'format', f, 5)).to eq(1.23)
      end
    end

    it 'runs for no endpoint specified (default behavior)' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "https://#{endpoint_import}/v3/table/import/db/table/format").
        with(:body => '12345').
        to_return(:body => '{"elapsed_time":"1.23"}')
      File.open(t.path) do |f|
        api_default.import('db', 'table', 'format', f, 5).should == 1.23
      end
    end

    it 'runs for no endpoint specified with ssl: false' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "http://#{endpoint_import}/v3/table/import/db/table/format").
        with(:body => '12345').
        to_return(:body => '{"elapsed_time":"1.23"}')
      File.open(t.path) do |f|
        api_default_http.import('db', 'table', 'format', f, 5).should == 1.23
      end
    end

    it 'runs for unknown endpoint specified' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "https://#{endpoint_unknown}/v3/table/import/db/table/format").
        with(:body => '12345').
        to_return(:body => '{"elapsed_time":"1.23"}')
      File.open(t.path) do |f|
        api_unknown_host.import('db', 'table', 'format', f, 5).should == 1.23
      end
    end

    it 'runs for unknown endpoint with ssl=false specified' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "http://#{endpoint_unknown}/v3/table/import/db/table/format").
        with(:body => '12345').
        to_return(:body => '{"elapsed_time":"1.23"}')
      File.open(t.path) do |f|
        api_unknown_host_http.import('db', 'table', 'format', f, 5).should == 1.23
      end
    end

    it 'raises APIError' do
      t = Tempfile.new('import_api_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, "https://#{endpoint_import}/v3/table/import/db/table/format").
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
