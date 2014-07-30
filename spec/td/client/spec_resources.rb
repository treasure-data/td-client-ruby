require 'spec_helper'
require 'td/client/model'

shared_context 'spec symbols' do
  let :apikey do
    '1/0123456789ABCDEFG'
  end

  let :db_name do
    'db_test'
  end

  let :table_name do
    'table_test'
  end

  let :sched_name do
    'sched_test'
  end

  let :result_name do
    'test'
  end

  let :bi_name do
    'bi_test'
  end

  let :cron do
    '* * * * *'
  end

  let :query do
    'select 1'
  end
  let :result_url do
    'td://@/test/table'
  end
end

shared_context 'database resources' do
  include_context 'common helper'

  let :db_names do
    [
      'cloud', 'yuffie', 'vincent', 'cid'
    ]
  end
end

shared_context 'job resources' do
  include_context 'database resources'

  MAX_JOB = 20

  let :job_types do
    [
      ['HiveJob', 'hive'],
      ['ExportJob', 'export'],
      ['BulkImportJob', 'bulk_import'],
      ['PartialDeleteJob', 'partialdelete']
    ]
  end

  let :raw_jobs do
    created_at = Time.at(1356966000)
    types = job_types
    dbs = db_names
    (0...MAX_JOB).map { |i|
      job_type = types[i % types.size]
      status = i.odd? ? 'success' : 'error'
      {
        "job_id"             => i,
        "url"                => "https://console.treasure-data.com/jobs/#{i.to_s}?target=query",
        "database"           => dbs[i % dbs.size].to_s,
        "status"             => status,
        "type"               => job_type[0].to_sym,
        "query"              => "select #{i}",
        "priority"           => i % 3,
        "result"             => nil,
        "created_at"         => created_at.to_s,
        "updated_at"         => (created_at + (i * 10)).to_s,
        "start_at"           => (created_at + (i * 10 * 60)).to_s,
        "end_at"             => (created_at + (i * 10 * 3600)).to_s,
        "cpu_time"           => i * 100 + i,
        "result_size"        => i * 1000,
        'retry_limit'        => 10,
        'organization'       => nil,
        'hive_result_schema' => nil,
        'debug' => {
          'stderr' => "job #{i} #{status}",
          'cmdout' => "job #{i} command",
        }
      }
    }
  end
end
