class TreasureData::API
module Result

  ####
  ## Result API
  ##

  # @return [Array<String>]
  def list_result
    code, body, res = get("/v3/result/list")
    if code != "200"
      raise_error("List result table failed", res)
    end
    js = checked_json(body, %w[results])
    result = []
    js['results'].map {|m|
      result << [m['name'], m['url'], nil] # same as database
    }
    return result
  end

  # @param [String] name
  # @param [String] url
  # @param [Hash] opts
  # @return [true]
  def create_result(name, url, opts={})
    params = {'url'=>url}.merge(opts)
    code, body, res = post("/v3/result/create/#{e name}", params)
    if code != "200"
      raise_error("Create result table failed", res)
    end
    return true
  end

  # @param [String] name
  # @return [true]
  def delete_result(name)
    code, body, res = post("/v3/result/delete/#{e name}")
    if code != "200"
      raise_error("Delete result table failed", res)
    end
    return true
  end

end
end
