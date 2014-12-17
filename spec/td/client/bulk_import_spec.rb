require 'spec_helper'
require 'td/client/spec_resources'
require 'tempfile'

describe 'BulkImport API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  let :packed do
    s = StringIO.new
    Zlib::GzipWriter.wrap(s) do |f|
      pk = MessagePack::Packer.new(f)
      pk.write([1, '2', 3.0])
      pk.write([4, '5', 6.0])
      pk.write([7, '8', 9.0])
      pk.flush
    end
    s.string
  end

  describe 'create_bulk_import' do
    it 'should create a new bulk_import' do
      stub_api_request(:post, "/v3/bulk_import/create/#{e(bi_name)}/#{e(db_name)}/#{e(table_name)}").
        to_return(:body => {'bulk_import' => bi_name}.to_json)

      api.create_bulk_import(bi_name, db_name, table_name).should be_nil
    end

    it 'should return 422 error with invalid name' do
      name = 'D'
      err_msg = "Validation failed: Name is too short" # " (minimum is 3 characters)"
      stub_api_request(:post, "/v3/bulk_import/create/#{e(name)}/#{e(db_name)}/#{e(table_name)}").
        to_return(:status => 422, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_bulk_import(name, db_name, table_name)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    it 'should return 404 error with non exist database name' do
      db = 'no_such_db'
      err_msg = "Couldn't find UserDatabase with name = #{db}"
      stub_api_request(:post, "/v3/bulk_import/create/#{e(bi_name)}/#{e(db)}/#{e(table_name)}").
        to_return(:status => 404, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_bulk_import(bi_name, db, table_name)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end

    it 'should return 404 error with non exist table name' do
      table = 'no_such_table'
      err_msg = "Couldn't find UserTableReference with name = #{table}"
      stub_api_request(:post, "/v3/bulk_import/create/#{e(bi_name)}/#{e(db_name)}/#{e(table)}").
        to_return(:status => 404, :body => {'message' => err_msg}.to_json)

      expect {
        api.create_bulk_import(bi_name, db_name, table)
      }.to raise_error(TreasureData::APIError, /#{err_msg}/)
    end
  end

  describe 'delete_bulk_import' do
    it 'runs' do
      stub_api_request(:post, '/v3/bulk_import/delete/name').
        with(:body => 'foo=bar')
      api.delete_bulk_import('name', 'foo' => 'bar').should == nil
    end
  end

  describe 'show_bulk_import' do
    it 'runs' do
      stub_api_request(:get, '/v3/bulk_import/show/name').
        to_return(:body => {'status' => 'status', 'other' => 'other'}.to_json)
      api.show_bulk_import('name')['status'].should == 'status'
    end
  end

  describe 'list_bulk_imports' do
    it 'runs' do
      stub_api_request(:get, '/v3/bulk_import/list').
        with(:query => 'foo=bar').
        to_return(:body => {'bulk_imports' => %w(1 2 3)}.to_json)
      api.list_bulk_imports('foo' => 'bar').should == %w(1 2 3)
    end
  end

  describe 'list_bulk_import_parts' do
    it 'runs' do
      stub_api_request(:get, '/v3/bulk_import/list_parts/name').
        with(:query => 'foo=bar').
        to_return(:body => {'parts' => %w(1 2 3)}.to_json)
      api.list_bulk_import_parts('name', 'foo' => 'bar').should == %w(1 2 3)
    end
  end

  describe 'bulk_import_upload_part' do
    it 'runs' do
      t = Tempfile.new('bulk_import_spec')
      File.open(t.path, 'w') do |f|
        f << '12345'
      end
      stub_request(:put, 'http://api.treasure-data.com/v3/bulk_import/upload_part/name/part').
        with(:body => '12345')
      File.open(t.path) do |f|
        api.bulk_import_upload_part('name', 'part', f, 5).should == nil
      end
    end
  end

  describe 'bulk_import_delete_part' do
    it 'runs' do
      stub_api_request(:post, '/v3/bulk_import/delete_part/name/part')
      api.bulk_import_delete_part('name', 'part').should == nil
    end
  end

  describe 'freeze_bulk_import' do
    it 'runs' do
      stub_api_request(:post, '/v3/bulk_import/freeze/name')
      api.freeze_bulk_import('name').should == nil
    end
  end

  describe 'unfreeze_bulk_import' do
    it 'runs' do
      stub_api_request(:post, '/v3/bulk_import/unfreeze/name')
      api.unfreeze_bulk_import('name').should == nil
    end
  end

  describe 'perform_bulk_import' do
    it 'runs' do
      stub_api_request(:post, '/v3/bulk_import/perform/name').
        to_return(:body => {'job_id' => 12345}.to_json)
      api.perform_bulk_import('name').should == '12345'
    end
  end

  describe 'commit_bulk_import' do
    it 'runs' do
      stub_api_request(:post, '/v3/bulk_import/commit/name').
        to_return(:body => {'job_id' => 12345}.to_json)
      api.commit_bulk_import('name').should == nil
    end
  end

  describe 'bulk_import_error_records' do
    it 'returns [] on empty' do
      stub_api_request(:get, '/v3/bulk_import/error_records/name').
        to_return(:body => '')
      api.bulk_import_error_records('name').should == []
    end

    it 'returns nil on empty if block given' do
      stub_api_request(:get, '/v3/bulk_import/error_records/name').
        to_return(:body => '')
      api.bulk_import_error_records('name'){}.should == nil
    end

    it 'returns unpacked result' do
      stub_api_request(:get, '/v3/bulk_import/error_records/name').
        to_return(:body => packed)
      api.bulk_import_error_records('name').should == [[1, '2', 3.0], [4, '5', 6.0], [7, '8', 9.0]]
    end

    it 'yields unpacked result if block given' do
      stub_api_request(:get, '/v3/bulk_import/error_records/name').
        to_return(:body => packed)
      result = []
      api.bulk_import_error_records('name') do |row|
        result << row
      end
      result.should == [[1, '2', 3.0], [4, '5', 6.0], [7, '8', 9.0]]
    end
  end
end
