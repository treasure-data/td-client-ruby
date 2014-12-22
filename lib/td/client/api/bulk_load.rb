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

  class ToHashStruct < Struct
    def to_h
      self.class.members.inject({}) { |r, k|
        v = self[k]
        r[k.to_s] = v.respond_to?(:to_h) ? v.to_h : v
        r
      }
    end
  end

  class Job < ToHashStruct.new(:config)
    def to_json
      to_h.to_json
    end

    def self.from_json(json)
      hash = JSON.load(json)
      new(JobConfig.from_hash(hash['config']))
    end
  end

  class JobConfig < ToHashStruct.new(:type, :access_key_id, :secret_access_key, :endpoint, :bucket, :paths, :parser)
    def self.from_hash(hash)
      new(*hash.values_at(*JobConfig.members.map(&:to_s)))
    end
  end

  class JobPreview < ToHashStruct.new(:schema, :records)
    def self.from_json(json)
      hash = JSON.load(json)
      JobPreview.new(hash['schema'], hash['records'])
    end
  end

  ## API definitions

  # job: Job -> Job
  def bulk_load_guess(job)
    # retry_request = true
    res = api { post('/v3/bulk_loads/guess', job.to_json) }
    unless res.ok?
      raise_error('BulkLoad configuration guess failed', res)
    end
    Job.from_json(res.body)
  end

  # job: Job -> JobPreview
  def bulk_load_preview(job)
    # retry_request = true
    res = api { post('/v3/bulk_loads/preview', job.to_json) }
    unless res.ok?
      raise_error('BulkLoad configuration preview failed', res)
    end
    JobPreview.from_json(res.body)
  end

end
end
