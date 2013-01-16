require 'spec_helper'
require 'td/client/model'

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
        "job_id"      => i,
        "url"         => "https://console.treasure-data.com/jobs/#{i.to_s}?target=query",
        "database"    => dbs[i % dbs.size].to_s,
        "status"      => status,
        "type"        => job_type[0].to_sym,
        "query"       => "select #{i}",
        "priority"    => i % 3,
        "result"      => nil,
        "created_at"  => created_at.to_s,
        "updated_at"  => (created_at + (i * 10)).to_s,
        "start_at"    => (created_at + (i * 10 * 60)).to_s,
        "end_at"      => (created_at + (i * 10 * 3600)).to_s,
        'organization' => nil,
        'hive_result_schema' => nil,
        'debug' => { 
          'stderr' => "job #{i} #{status}",
          'cmdout' => "job #{i} command",
        }
      }
    }
  end
end
