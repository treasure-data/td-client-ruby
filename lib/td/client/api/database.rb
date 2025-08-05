class TreasureData::API
module Database

  ####
  ## Database API
  ##

  # @return [Array<String>] names as array
  def list_databases
    code, body, res = get("/v3/database/list")
    if code != "200"
      raise_error("List databases failed", res)
    end
    js = checked_json(body, %w[databases])
    result = {}
    js["databases"].each {|m|
      name = m['name']
      count = m['count']
      created_at = m['created_at']
      updated_at = m['updated_at']
      permission = m['permission']
      result[name] = [count, created_at, updated_at, nil, permission] # set nil to org for API compatibiilty
    }
    return result
  end

  # @param [String] db
  # @return [true]
  def delete_database(db)
    code, body, res = post("/v3/database/delete/#{e db}")
    if code != "200"
      raise_error("Delete database failed", res)
    end
    return true
  end

  # @param [String] db
  # @param [Hash] opts
  # @return [true]
  def create_database(db, opts={})
    params = opts.dup
    code, body, res = post("/v3/database/create/#{e db}", params)
    if code != "200"
      raise_error("Create database failed", res)
    end
    return true
  end

end
end
