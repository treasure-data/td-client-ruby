class TreasureData::API
module BulkLoad

  ####
  ## BulkLoad (Server-side Bulk loader) API
  ##

  #  1. POST   /v3/bulk_loads/guess          - stateless non REST API to return guess result as BulkLoadSession [NEW]
  #  2. POST   /v3/bulk_loads/preview        - stateless non REST API to return preview result as BulkLoadSession [NEW]
  #
  #  3. POST   /v3/job/issue/:type/:database - create a job resource to run BulkLoadSession [EXTENDED]
  #  4. POST   /v3/job/kill/:id              - kill the job [ALREADY EXISTS]
  #  5. GET    /v3/job/show/:id              - get status of the job [ALREADY EXISTS]
  #  6. GET    /v3/job/result/:id            - get result of the job [NOT NEEDED IN Q4] ... because backend feature is not yet implemented
  #
  #  7. GET    /v3/bulk_loads                - list BulkLoadSession resources [NEW]
  #  8. POST   /v3/bulk_loads                - create BulkLoadSession [NEW]
  #  9. GET    /v3/bulk_loads/:name          - get BulkLoadSession [NEW]
  # 10. PUT    /v3/bulk_loads/:name          - update BulkLoadSession [NEW]
  # 11. DELETE /v3/bulk_loads/:name          - delete BulkLoadSession [NEW]
  # 12. GET    /v3/bulk_loads/:name/jobs     - list BulkLoadSession job history [NEW]
  # 13. POST   /v3/bulk_loads/:name/jobs     - run BulkLoadSession [NEW]

  # The 'BulkLoadSession' resource in td-api is as follows;
  # {
  #   "config": {
  #     "type": "s3",
  #     "access_key_id": s3 access key id,
  #     "secret_access_key": s3 secret key,
  #     "endpoint": s3 endpoint name,
  #     "bucket": s3 bucket name,
  #     "path_prefix": "a/prefix/of/files",
  #     "decoders": []
  #   },
  #   "name": account_wide_unique_name,
  #   "cron": cron_string,
  #   "timezone": timezone_string,
  #   "delay": delay_seconds,
  #   "database": database_name,
  #   "table": table_name
  # }

  LIST = '/v3/bulk_loads'
  SESSION = LIST +     '/%s'
  JOB = SESSION +         '/jobs'

  # job: Hash -> Hash
  def bulk_load_guess(job)
    # retry_request = true
    path = LIST + '/guess'
    res = api { post(path, job.to_json) }
    unless res.ok?
      raise_error('BulkLoad configuration guess failed', res)
    end
    JSON.load(res.body)
  end

  # job: Hash -> Hash
  def bulk_load_preview(job)
    # retry_request = true
    path = LIST + '/preview'
    res = api { post(path, job.to_json) }
    unless res.ok?
      raise_error('BulkLoad job preview failed', res)
    end
    JSON.load(res.body)
  end

  # job: Hash -> String (job_id)
  def bulk_load_issue(database, table, job)
    type = 'bulkload'
    job = job.dup
    job['database'] = database
    job['table'] = table
    path = "/v3/job/issue/#{e type}/#{e database}"
    res = api { post(path, job.to_json) }
    unless res.ok?
      raise_error('BulkLoad job issuing failed', res)
    end
    js = checked_json(res.body)
    js['job_id'].to_s
  end

  # nil -> [Hash]
  def bulk_load_list
    res = api { get(LIST) }
    unless res.ok?
      raise_error("BulkLoadSession list retrieve failed", res)
    end
    JSON.load(res.body)
  end

  # name: String, database: String, table: String, job: Hash -> Hash
  def bulk_load_create(name, database, table, job, opts = {})
    job = job.dup
    job['name'] = name
    [:cron, :timezone, :delay, :time_column].each do |prop|
      job[prop.to_s] = opts[prop] if opts.key?(prop)
    end
    job['database'] = database
    job['table'] = table
    res = api { post(LIST, job.to_json) }
    unless res.ok?
      raise_error("BulkLoadSession: #{name} create failed", res)
    end
    JSON.load(res.body)
  end

  # name: String -> Hash
  def bulk_load_show(name)
    path = session_path(name)
    res = api { get(path) }
    unless res.ok?
      raise_error("BulkLoadSession: #{name} retrieve failed", res)
    end
    JSON.load(res.body)
  end

  # name: String, settings: Hash -> Hash
  def bulk_load_update(name, settings)
    path = session_path(name)
    res = api { put(path, settings.to_json) }
    unless res.ok?
      raise_error("BulkLoadSession: #{name} update failed", res)
    end
    JSON.load(res.body)
  end

  # name: String -> Hash
  def bulk_load_delete(name)
    path = session_path(name)
    res = api { delete(path) }
    unless res.ok?
      raise_error("BulkLoadSession: #{name} delete failed", res)
    end
    JSON.load(res.body)
  end

  # name: String -> [Hash]
  def bulk_load_history(name)
    path = job_path(name)
    res = api { get(path) }
    unless res.ok?
      raise_error("history of BulkLoadSession: #{name} retrieve failed", res)
    end
    JSON.load(res.body)
  end

  def bulk_load_run(name, scheduled_time = nil)
    path = job_path(name)
    opts = {}
    opts[:scheduled_time] = scheduled_time.to_s unless scheduled_time.nil?
    res = api { post(path, opts.to_json) }
    unless res.ok?
      raise_error("BulkLoadSession: #{name} job create failed", res)
    end
    js = checked_json(res.body)
    js['job_id'].to_s
  end

private

  def session_path(name)
    SESSION % e(name)
  end

  def job_path(name)
    JOB % e(name)
  end

end
end
