require 'td/client/api/to_hash_struct'

class TreasureData::API
module BulkLoad

  ####
  ## BulkLoad (Server-side Bulk loader) API
  ##

  # 1. post /bulk_loads/guess - stateless non REST API to return guess result [NEW]
  # 2. post /bulk_loads/preview - stateless non REST API to return preview result [NEW]
  # 3. post /job/issue/:type/:database - create a Job record to run Bulkload Worker [EXTENDED]
  # 4. post /job/kill/:id - kill the job [ALREADY EXISTS]
  # 5. get /job/show/:id - get status of the job [ALREADY EXISTS]
  # 6. get /job/result/:id - get result of the job [NOT NEEDED IN Q4] ... because backend feature is not yet implemented

  # The 'BulkLoad' resource in td-api is as follow;
  # {
  #   "config": {
  #     "type": "s3",
  #     "access_key_id": s3 access key id,
  #     "secret_access_key": s3 secret key,
  #     "endpoint": s3 endpoint name,
  #     "bucket": s3 bucket name,
  #     "paths": [
  #       a prefix of files,
  #       or path to a file,
  #       ...
  #     ]
  #   }  
  # }

  ## Resource definitions

  class Job < ToHashStruct.new(:config, :database, :table)
    class JobConfig < ToHashStruct.new(:type, :access_key_id, :secret_access_key, :endpoint, :bucket, :paths, :parser)
      def validate_self
        validate_presence_of :type
      end
    end

    model_property :config, JobConfig

    def validate_self
      validate_presence_of :config
      validate_presence_of :database
      validate_presence_of :table
    end
  end

  class JobPreview < ToHashStruct.new(:schema, :records)
  end

  ## API definitions

  # job: Job -> Job
  def bulk_load_guess(job)
    # retry_request = true
    res = api { post('/v3/bulk_loads/guess', job.validate.to_json) }
    unless res.ok?
      raise_error('BulkLoad configuration guess failed', res)
    end
    Job.from_json(res.body)
  end

  # job: Job -> JobPreview
  def bulk_load_preview(job)
    # retry_request = true
    res = api { post('/v3/bulk_loads/preview', job.validate.to_json) }
    unless res.ok?
      raise_error('BulkLoad job preview failed', res)
    end
    JobPreview.from_json(res.body)
  end

  # job: Job -> String (job_id)
  def bulk_load_issue(job)
    type = e('bulkload')
    db = e(job['database']) # TODO: not required?
    res = api { post("/v3/job/issue/#{type}/#{db}", job.validate.to_json) }
    unless res.ok?
      raise_error('BulkLoad job issuing failed', res)
    end
    js = checked_json(res.body)
    js['job_id'].to_s
  end

end
end
