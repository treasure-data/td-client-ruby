class TreasureData::API
module PartialDelete

  ####
  ## Partial delete API
  ##

  # @param [String] db
  # @param [String] table
  # @param [Fixnum] to
  # @param [Fixnum] from
  # @param [Hash] opts
  # @return [String]
  def partial_delete(db, table, to, from, opts={})
    params = opts.dup
    params['to'] = to.to_s
    params['from'] = from.to_s
    code, body, res = post("/v3/table/partialdelete/#{e db}/#{e table}", params)
    if code != "200"
      raise_error("Partial delete failed", res)
    end
    js = checked_json(body, %w[job_id])
    return js['job_id'].to_s
  end

end
end
