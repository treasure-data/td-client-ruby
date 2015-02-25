require 'spec_helper'
require 'td/client/spec_resources'
require 'json'

describe 'Model.rb' do
  include_context 'spec symbols'
  include_context 'common helper'

  let(:created_at) { '2014-12-14T17:24:00+0900' }
  let(:updated_at) { '2014-12-14T18:25:00+0900' }

  let :api do
    API.new(nil)
  end

  let :client do
    Client.new(apikey)
  end

  shared_examples_for :created_at_with_existing_check do
    context 'created_at exists' do
      let(:created_at) { '2014-12-14T17:24:00+0900' }

      it 'is parsed as Time objact' do
        expect(subject.created_at).to eq Time.parse(attr[:created_at])
      end
    end

    context 'created_at is nil' do
      let(:created_at) { nil }

      it 'returns nil' do
        expect(subject.created_at).to be_nil
      end
    end

    context 'created_at is nil' do
      let(:created_at) { '' }

      it 'returns nil' do
        expect(subject.created_at).to be_nil
      end
    end
  end

  shared_examples_for :updated_at_with_existing_check do
    context 'updated_at exists' do
      let(:updated_at) { '2014-12-14T17:24:00+0900' }

      it 'is parsed as Time objact' do
        expect(subject.updated_at).to eq Time.parse(attr[:updated_at])
      end
    end

    context 'updated_at is nil' do
      let(:updated_at) { nil }

      it 'returns nil' do
        expect(subject.updated_at).to be_nil
      end
    end

    context 'updated_at is nil' do
      let(:updated_at) { '' }

      it 'returns nil' do
        expect(subject.updated_at).to be_nil
      end
    end
  end

  describe 'Model' do
    describe 'initialize' do
      it { expect(TreasureData::Model.new(client).client).to eq client }
    end
  end

  describe 'Account' do
    let(:created_at) { '2014-12-14T17:24:00+0900' }
    let(:storage_size) { 1024 * 1024 }

    let(:attr) do
      {
        :client => client,
        :account_id => 1,
        :plan => 0,
        :storage_size => storage_size,
        :guaranteed_cores => 3,
        :maximum_cores => 4,
        :created_at => created_at,
      }
    end

    subject { TreasureData::Account.new(attr[:client], attr[:account_id], attr[:plan], attr[:storage_size], attr[:guaranteed_cores], attr[:maximum_cores], attr[:created_at] ) }

    describe 'initialize' do
      it 'saves arges as instance variable' do
        attr.each do |k,v|
          expect(subject.instance_variable_get("@#{k}")).to eq v
        end
      end

      it 'can access each attributes as reader' do
        attr.each do |k,v|
          next if k == :created_at
          expect(subject.send(k)).to eq v
        end
      end
    end

    it_should_behave_like :created_at_with_existing_check

    describe '#storage_size_string' do
      context 'storage_size <= 1024 * 1024' do
        let(:storage_size) { 1024 * 1024 }
        it { expect(subject.storage_size_string).to eq "0.0 GB" }
      end

      context 'storage_size <= 60 * 1024 * 1024' do
        let(:storage_size) { 60 * 1024 * 1024 }
        it { expect(subject.storage_size_string).to eq "0.01 GB" }
      end

      context 'storage_size <= 60 * 1024 * 1024 * 1024' do
        let(:storage_size) { 60 * 1024 * 1024 * 1024 }
        it { expect(subject.storage_size_string).to eq "60.0 GB" }
      end

      context 'storage_size <= 60 * 1024 * 1024 * 1024 * 60' do
        let(:storage_size) { (60 * 1024 * 1024 * 1024 * 60) }
        it { expect(subject.storage_size_string).to eq "3600 GB" }
      end
    end
  end

  describe 'Database' do
    let(:table) { nil }

    let(:attr) do
      {
        :client => client,
        :db_name => db_name,
        :tables => table,
        :count=> 3,
        :created_at => created_at,
        :updated_at => updated_at,
        :org_name => "org_name",
        :permission => "query_only"
      }
    end

    subject { TreasureData::Database.new(attr[:client], attr[:db_name], attr[:tables], attr[:count], attr[:created_at], attr[:updated_at], attr[:org_name], attr[:permission] ) }

    describe '.initialize' do
      it 'saves arges as instance variable' do
        attr.each do |k,v|
          next if k == :org_name

          v = v.to_sym if k == :permission
          expect(subject.instance_variable_get("@#{k}")).to eq v
        end
      end

      it 'can access each attributes as reader' do
        expect(subject.permission).to eq attr[:permission].to_sym
        expect(subject.count).to eq attr[:count]
      end
    end

    describe '#name' do
      it { expect(subject.name).to eq attr[:db_name] }
    end

    describe '#tables' do
      let(:tables) {
        [
          ["table_1", "item", "[[\"time\",\"long\"],[\"value\",\"string\"]]", 111, "2013-01-21 01:51:41 UTC", "2014-01-21 01:51:41 UTC"],
          ["table_2", "log",  "[[\"time\",\"long\"],[\"value\",\"long\"]]",   222, "2013-02-22 02:52:42 UTC", "2014-02-22 02:52:42 UTC"],
          ["table_3", "item", "[[\"time\",\"long\"],[\"value\",\"string\"]]", 333, "2013-03-23 03:53:43 UTC", "2014-03-23 03:53:43 UTC"],
          ["table_4", "log",  "[[\"time\",\"long\"],[\"value\",\"long\"]]",   444, "2013-04-24 04:54:44 UTC", "2014-04-24 04:54:44 UTC"]
        ]
      }

      context 'client.tables are empty' do
        it 'tables are also empty' do
          stub_request(:get, "http://api.treasure-data.com/v3/table/list/#{db_name}").
            with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'deflate, gzip', 'Authorization'=>'TD1 1/0123456789ABCDEFG', 'Date'=>/.*/, 'User-Agent' => /Ruby/}).
            to_return(:status => 200, :body => {'tables' => [] }.to_json, :headers => {})

            expect(subject.tables).to eq []
        end
      end

      context 'client.tables has some table info' do
        let(:table_info) {
          {'tables' => [
            {'name' => tables[0][0], 'type' => tables[0][1], 'schema' => tables[0][2], 'count' => tables[0][3], 'created_at' => tables[0][4], 'updated_at' => tables[0][5]},
            {'name' => tables[1][0], 'type' => tables[1][1], 'schema' => tables[1][2], 'count' => tables[1][3], 'created_at' => tables[1][4], 'updated_at' => tables[1][5]},
            {'name' => tables[2][0], 'type' => tables[2][1], 'schema' => tables[2][2], 'count' => tables[2][3], 'created_at' => tables[2][4], 'updated_at' => tables[2][5]},
            {'name' => tables[3][0], 'type' => tables[3][1], 'schema' => tables[3][2], 'count' => tables[3][3], 'created_at' => tables[3][4], 'updated_at' => tables[3][5]}
          ]}.to_json
        }

        it 'has tables info' do
          stub_request(:get, "http://api.treasure-data.com/v3/table/list/#{db_name}").
            with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'deflate, gzip', 'Authorization'=>'TD1 1/0123456789ABCDEFG', 'Date'=>/.*/, 'User-Agent' => /Ruby/}).
            to_return(:status => 200, :body => table_info, :headers => {})

            expect(subject.tables.size).to eq 4
        end
      end
    end

    it_should_behave_like :created_at_with_existing_check
    it_should_behave_like :updated_at_with_existing_check

    describe '#create_log_table' do
      let!(:name) { 'test_log_table' }

      it 'calls client#create_log_table' do
        allow(client).to receive(:create_log_table).with(db_name, name)
        subject.create_log_table(name)
      end
    end

    describe '#create_item_table' do
      let!(:name) { 'test_item_table' }

      it 'calls client#create_item_table' do
        allow(client).to receive(:create_item_table).with(db_name, name)
        subject.create_item_table(name)
      end
    end

    describe '#table' do
      let!(:table_name) { 'test_table_name' }

      it 'calls client#table' do
        allow(client).to receive(:table).with(db_name, table_name)
        subject.table(table_name)
      end
    end

    describe '#delete' do
      it 'calls client#delete' do
        allow(client).to receive(:delete_database).with(db_name)
        subject.delete
      end
    end

    describe '#query' do
      let!(:query_str) { 'query_str' }

      it 'calls client#query' do
        allow(client).to receive(:query).with(db_name, query_str)
        subject.query(query_str)
      end
    end
  end

  describe 'Table' do
    let(:table_name) { 'test_table_name' }
    let(:estimated_storage_size) { 3 }
    let(:type) { "item" }
    let(:schema) {  "[[\"time\",\"long\"],[\"value\",\"string\"]]" }
    let(:last_import) { nil }
    let(:last_log_timestamp) { nil }
    let(:expire_days) { nil }

    let(:attr) do
      {
        :client => client,
        :db_name => db_name,
        :table_name => table_name,
        :type => type,
        :schema => schema,
        :count=> 3,
        :created_at => created_at,
        :updated_at => updated_at,
        :estimated_storage_size => estimated_storage_size,
        :last_import => last_import,
        :last_log_timestamp => last_log_timestamp,
        :expire_days => expire_days,
        :primary_key => nil,
      }
    end

    subject do
      TreasureData::Table.new(
        attr[:client],
        attr[:db_name],
        attr[:table_name],
        attr[:type],
        attr[:schema],
        attr[:count],
        attr[:created_at],
        attr[:updated_at],
        attr[:estimated_storage_size],
        attr[:last_import],
        attr[:last_log_timestamp],
        attr[:expire_days],
        attr[:primary_key],
      )
    end

    describe '#last_import' do
      context '@last_import is nil' do
        let(:last_import) { nil }
        it { expect(subject.last_import).to be_nil }
      end

      context '@last_import is empty' do
        let(:last_import) { "" }
        it { expect(subject.last_import).to be_nil }
      end

      context '@last_import is not empty' do
        let(:last_import) { '2014-12-13T18:25:00+0900' }
        it { expect(subject.last_import).to eq Time.parse(last_import) }
      end
    end

    describe '#last_log_timestamp' do
      context '@last_log_timestamp is nil' do
        let(:last_log_timestamp) { nil }
        it { expect(subject.last_log_timestamp).to be_nil }
      end

      context '@last_log_timestamp is empty' do
        let(:last_log_timestamp) { "" }
        it { expect(subject.last_log_timestamp).to be_nil }
      end

      context '@last_log_timestamp is not empty' do
        let(:last_log_timestamp) { '2014-12-13T18:25:00+0900' }
        it { expect(subject.last_log_timestamp).to eq Time.parse(last_log_timestamp) }
      end
    end

    describe '#expire_days' do
      context '@expire_days is nil' do
        let(:expire_days) { nil }
        it { expect(subject.expire_days).to be_nil }
      end

      context '@expire_days is not empty' do
        let(:expire_days) { '3' }
        it { expect(subject.expire_days).to eq expire_days.to_i }
      end
    end

    describe '#expire_days' do
      context '@expire_days is nil' do
        let(:expire_days) { nil }
        it { expect(subject.expire_days).to be_nil }
      end

      context '@expire_days is not empty' do
        let(:expire_days) { '3' }
        it { expect(subject.expire_days).to eq expire_days.to_i }
      end
    end

    describe '#identifier' do
      it { expect(subject.identifier).to eq "db_test.test_table_name" }
    end

    describe '#delete' do
      it 'calls client#delete' do
        allow(client).to receive(:delete_table).with(db_name, table_name)
        subject.delete
      end
    end

    describe '#tail' do
      let(:count) { 3 }
      let(:to) { 100 }
      let(:from) { 10 }

      it 'calls client#tail' do
        allow(client).to receive(:tail).with(db_name, table_name, count, to, from)
        subject.tail(count, to, from)
      end
    end

    describe '#import' do
      let(:format) { 3 }
      let(:stream) { StringIO.new "---" }
      let(:size) { 10 }

      it 'calls client#import' do
        allow(client).to receive(:import).with(db_name, table_name, format, stream, size)
        subject.import(format, stream, size)
      end
    end

    describe '#export' do
      let(:storage_type) { 3 }
      let(:opts) { { } }

      it 'calls client#export' do
        allow(client).to receive(:export).with(db_name, table_name, storage_type, opts)
        subject.export(storage_type, opts)
      end
    end

    describe '#estimated_storage_size_string' do
      context 'storage_size <= 1024 * 1024' do
        let(:estimated_storage_size) { 1024 * 1024 }
        it { expect(subject.estimated_storage_size_string).to eq "0.0 GB" }
      end

      context 'storage_size <= 60 * 1024 * 1024' do
        let(:estimated_storage_size) { 60 * 1024 * 1024 }
        it { expect(subject.estimated_storage_size_string).to eq "0.01 GB" }
      end

      context 'storage_size <= 60 * 1024 * 1024 * 1024' do
        let(:estimated_storage_size) { 60 * 1024 * 1024 * 1024 }
        it { expect(subject.estimated_storage_size_string).to eq "60.0 GB" }
      end

      context 'storage_size <= 60 * 1024 * 1024 * 1024 * 60' do
        let(:estimated_storage_size) { (60 * 1024 * 1024 * 1024 * 60) }
        it { expect(subject.estimated_storage_size_string).to eq "3600 GB" }
      end
    end

    describe '#update_database!' do
      it 'calls client#database' do
        allow(client).to receive(:database).with(db_name)
        subject.update_database!
      end
    end

    it_should_behave_like :created_at_with_existing_check
    it_should_behave_like :updated_at_with_existing_check
  end

  describe 'Schema' do
    let!(:cols) { 'name:type,name2:type2,name3:type3' }
    let!(:schema)  { Schema.parse(cols) }

    describe '.parse' do
      subject { schema }

      it 'returns own instance' do
        expect(subject.class).to eq Schema
      end

      it 'has files' do
        expect(subject.fields.first.name).to eq 'name'
        expect(subject.fields.first.type).to eq 'type'

        expect(subject.fields[1].name).to eq 'name2'
        expect(subject.fields[1].type).to eq 'type2'

        expect(subject.fields[2].name).to eq 'name3'
        expect(subject.fields[2].type).to eq 'type3'
      end
    end

    describe '#add_field' do
      let(:add_name) { 'add_name' }
      let(:add_type) { 'add_type' }

      before do
        schema.add_field(add_name, add_type)
      end

      subject { schema.fields.last }

      it { expect(subject.name).to eq add_name }
      it { expect(subject.type).to eq add_type }
    end

    describe '#merge(schema)' do
      let(:schema2) { Schema.parse(cols2) }

      subject { schema.merge(schema2) }

      context 'two schemas have doubled field' do
        let(:cols2) { 'name:other_type,name3:other_type3,name100:other_type100' }

        it 'double filelds types are wrote over by schema2' do
          expect(subject.fields.find { |f| f.name == 'name' }.type).to eq 'other_type'
          expect(subject.fields.find { |f| f.name == 'name3' }.type).to eq 'other_type3'
        end

        it 'the filelds which only schema2 has are added' do
          expect(subject.fields.find { |f| f.name == 'name100' }.type).to eq 'other_type100'
        end

        it 'the filelds which only schema has are remain' do
          expect(subject.fields.find { |f| f.name == 'name2' }.type).to eq 'type2'
        end

        it { expect(subject.fields.size). to eq 4 }
      end

      context 'two schemas have not doubled field' do
        let(:cols2) { 'name101:other_type101' }

        it { expect(subject.fields.size).to eq 4 }
        it { expect(subject.fields.last.type).to eq 'other_type101' }
      end
    end
  end

 describe 'Job' do
   let(:job_id) { 1 }
   let(:type) { 'hive' }
   let(:query) {  {'from' => '0', 'to' => '10'} }
   let(:status) { Job::STATUS_QUEUED }
   let(:url) { 'http:://test.com' }
   let(:debug) { {}  }
   let(:start_at) { '2014-12-14T17:24:00+0900' }
   let(:end_at) { '2014-12-16T17:24:00+0900' }
   let(:cpu_time) { 100 }

   let(:attr) do
     {:client => client,
      :job_id => job_id,
      :type   => type,
      :query  => query,
      :status => status,
      :url => url,
      :debug => debug,
      :start_at => start_at,
      :end_at => end_at,
      :cpu_time => cpu_time,
      :result_size => nil,
      :result => nil,
      :result_url => nil,
      :hive_result_schema => nil,
      :priority => nil,
      :retry_limit => nil,
      :org_name => nil,
      :b_name => nil }
    end

    let(:job) { Job.new(attr[:client],
          attr[:job_id],
          attr[:type],
          attr[:query],
          attr[:status],
          attr[:url],
          attr[:debug],
          attr[:start_at],
          attr[:end_at],
          attr[:cpu_time],
          attr[:result_size],
          attr[:result],
          attr[:result_url],
          attr[:hive_result_schema],
          attr[:priority],
          attr[:retry_limit],
          attr[:org_name],
          attr[:db_name]
     ) }

    describe 'query' do
      context 'has @query' do
        it { expect(job.query).to eq query }
      end

      context 'has finished' do
        let(:status) { Job::STATUS_SUCCESS }
        let(:query) { nil }

        it { expect(job.query).to eq query }
      end

      context 'not finished yet, and @query is nil' do
        let(:query) { nil }
        it 'call update_status!' do
          allow(job).to receive(:update_status!)
          job.query
        end
      end
    end

    describe 'status' do
      context 'has @status' do
        it { expect(job.status).to eq status }
      end

      context 'has finished' do
        let(:status) { Job::STATUS_SUCCESS }

        it { expect(job.status).to eq status }
      end

      context 'not finished yet, and @status is nil' do
        let(:status) { nil }
        it 'call update_status!' do
          allow_any_instance_of(Job).to receive(:update_status!)
          job
        end
      end
    end

    describe 'url' do
      context 'has @url' do
        it { expect(job.url).to eq url }
      end

      context 'has finished' do
        let(:status) { Job::STATUS_SUCCESS }
        let(:url) { nil }

        it { expect(job.url).to eq url }
      end

      context 'not finished yet, and @url is nil' do
        let(:url) { nil }
        it 'call update_status!' do
          allow_any_instance_of(Job).to receive(:update_status!)
          job
        end
      end
    end

    describe 'debug' do
      context 'has @debug' do
        it { expect(job.debug).to eq debug }
      end

      context 'has finished' do
        let(:status) { Job::STATUS_SUCCESS }
        let(:debug) { nil }

        it { expect(job.debug).to eq debug }
      end

      context 'not finished yet, and @debug is nil' do
        let(:debug) { nil }
        it 'call update_status!' do
          allow_any_instance_of(Job).to receive(:update_status!)
          job
        end
      end
    end

    describe 'start_at' do
      context 'has @start_at' do
        context '@start_at is time-string' do
          it { expect(job.start_at).to eq Time.parse(start_at) }
        end

        context '@start_at is empty' do
          let(:start_at) { "" }
          it { expect(job.start_at).to be_nil }
        end
      end

      context 'has finished' do
        let(:status) { Job::STATUS_SUCCESS }
        let(:start_at) { nil }

        it { expect(job.start_at).to eq start_at }
      end

      context 'not finished yet, and @start_at is nil' do
        let(:start_at) { nil }
        it 'call update_status!' do
          allow_any_instance_of(Job).to receive(:update_status!).and_return(true)
          job
        end
      end
    end

    describe 'end_at' do
      context 'has @end_at' do
        context '@end_at is time-string' do
          it { expect(job.end_at).to eq Time.parse(end_at) }
        end

        context '@end_at is empty' do
          let(:end_at) { "" }
          it { expect(job.end_at).to be_nil }
        end
      end

      context 'has finished' do
        let(:status) { Job::STATUS_SUCCESS }
        let(:end_at) { nil }

        it { expect(job.end_at).to eq end_at }
      end

      context 'not finished yet, and @end_at is nil' do
        let(:end_at) { nil }
        it 'call update_status!' do
          allow_any_instance_of(Job).to receive(:update_status!).and_return(true)
          job
        end
      end
    end

    describe 'cpu_time' do
      context 'has @cpu_time' do
        it { expect(job.cpu_time).to eq cpu_time }
      end

      context 'has finished' do
        let(:status) { Job::STATUS_SUCCESS }
        let(:cpu_time) { nil }

        it { expect(job.cpu_time).to eq cpu_time }
      end

      context 'not finished yet, and @cpu_time is nil' do
        let(:cpu_time) { nil }
        it 'call update_status!' do
          allow_any_instance_of(Job).to receive(:update_status!)
          job
        end
      end
    end
  end
end
