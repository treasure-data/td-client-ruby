class TreasureData::API
module Export

  ####
  ## Export API
  ##

  # => jobId:String
  # @param [String] db
  # @param [String] table
  # @param [String] storage_type
  # @param [Hash] opts
  # @return [String] job_id
  def export(db, table, storage_type, opts={})
    params = opts.dup
    params['storage_type'] = storage_type
    code, body, res = post("/v3/export/run/#{e db}/#{e table}", params)
    if code != "200"
      raise_error("Export failed", res)
    end
    js = checked_json(body, %w[job_id])
    return js['job_id'].to_s
  end

  # => jobId:String
  # @param [String] target_job_id
  # @param [Hash] opts
  # @return [String] job_id
  def result_export(target_job_id, opts={})
    code, body, res = post("/v3/job/result_export/#{target_job_id}", opts)
    if code[0] != ?2
      raise_error("Result Export failed", res)
    end
    js = checked_json(body, %w[job_id])
    return js['job_id'].to_s
  end
end
end
