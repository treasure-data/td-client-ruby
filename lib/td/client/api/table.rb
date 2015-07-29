class TreasureData::API
module Table

  ####
  ## Table API
  ##

  # @param [String] db
  # @return [Array]
  def list_tables(db)
    code, body, res = get("/v3/table/list/#{e db}")
    if code != "200"
      raise_error("List tables failed", res)
    end
    js = checked_json(body, %w[tables])
    result = {}
    js["tables"].map {|m|
      name = m['name']
      type = (m['type'] || '?').to_sym
      count = (m['count'] || 0).to_i  # TODO?
      created_at = m['created_at']
      updated_at = m['updated_at']
      last_import = m['counter_updated_at']
      last_log_timestamp = m['last_log_timestamp']
      estimated_storage_size = m['estimated_storage_size'].to_i
      schema = JSON.parse(m['schema'] || '[]')
      expire_days = m['expire_days']
      result[name] = [type, schema, count, created_at, updated_at, estimated_storage_size, last_import, last_log_timestamp, expire_days]
    }
    return result
  end

  # @param [String] db
  # @param [String] table
  # @return [true]
  def create_log_table(db, table)
    create_table(db, table, :log)
  end

  # @param [String] db
  # @param [String] table
  # @param [String] type
  # @param [Hash] params
  # @return [true]
  def create_table(db, table, type, params={})
    schema = schema.to_s
    code, body, res = post("/v3/table/create/#{e db}/#{e table}/#{type}", params)
    if code != "200"
      raise_error("Create #{type} table failed", res)
    end
    return true
  end
  private :create_table

  # @param [String] db
  # @param [String] table1
  # @param [String] table2
  # @return [true]
  def swap_table(db, table1, table2)
    code, body, res = post("/v3/table/swap/#{e db}/#{e table1}/#{e table2}")
    if code != "200"
      raise_error("Swap tables failed", res)
    end
    return true
  end

  # @param [String] db
  # @param [String] table
  # @param [String] schema_json
  # @return [true]
  def update_schema(db, table, schema_json)
    code, body, res = post("/v3/table/update-schema/#{e db}/#{e table}", {'schema'=>schema_json})
    if code != "200"
      raise_error("Create schema table failed", res)
    end
    return true
  end

  # @param [String] db
  # @param [String] table
  # @param [Fixnum] expire_days
  # @return [true]
  def update_expire(db, table, expire_days)
    code, body, res = post("/v3/table/update/#{e db}/#{e table}", {'expire_days'=>expire_days})
    if code != "200"
      raise_error("Update table expiration failed", res)
    end
    return true
  end

  # @param [String] db
  # @param [String] table
  # @return [Symbol]
  def delete_table(db, table)
    code, body, res = post("/v3/table/delete/#{e db}/#{e table}")
    if code != "200"
      raise_error("Delete table failed", res)
    end
    js = checked_json(body, %w[])
    type = (js['type'] || '?').to_sym
    return type
  end

  # @param [String] db
  # @param [String] table
  # @param [Fixnum] count
  # @param [Proc] block
  # @return [Array, nil]
  def tail(db, table, count, to = nil, from = nil, &block)
    unless to.nil? and from.nil?
      warn('parameter "to" and "from" no longer work')
    end
    params = {'format' => 'msgpack'}
    params['count'] = count.to_s if count
    code, body, res = get("/v3/table/tail/#{e db}/#{e table}", params)
    if code != "200"
      raise_error("Tail table failed", res)
    end
    if block
      MessagePack::Unpacker.new.feed_each(body, &block)
      nil
    else
      result = []
      MessagePack::Unpacker.new.feed_each(body) {|row|
        result << row
      }
      return result
    end
  end

end
end
