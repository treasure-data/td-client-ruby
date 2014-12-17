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

  def bulk_load_guess(config)
    # TODO: This request sends request in application/x-www-form-urlencoded. application/json?
    code, body, res = post('/v3/bulk_loads/guess', :guess => config.to_json)
    if code != '200'
      raise_error('BulkLoad configuration guess failed', res)
    end
    checked_json(body)
  end

  def bulk_load_preview(config)
    code, body, res = post('/v3/bulk_loads/preview', :preview => config.to_json)
    if code != '200'
      raise_error('BulkLoad configuration preview failed', res)
    end
    checked_json(body)
  end

end
end
