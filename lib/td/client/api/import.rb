class TreasureData::API
module Import

  ####
  ## Import API
  ##

  # @param [String] db
  # @param [String] table
  # @param [String] format
  # @param [String, StringIO] stream
  # @param [Fixnum] size
  # @param [String] unique_id
  # @return [Float] elapsed time
  def import(db, table, format, stream, size, unique_id=nil)
    if unique_id
      path = "/v3/table/import_with_id/#{e db}/#{e table}/#{unique_id}/#{format}"
    else
      path = "/v3/table/import/#{e db}/#{e table}/#{format}"
    end
    opts = {}
    if @host == DEFAULT_ENDPOINT
      opts[:host] = DEFAULT_IMPORT_ENDPOINT
    elsif @host == 'api.treasure-data.com' # backward compatibility
      opts[:host] = 'api-import.treasure-data.com'
      opts[:ssl] = false
    end
    code, body, res = put(path, stream, size, opts)
    if code[0] != ?2
      raise_error("Import failed", res)
    end
    js = checked_json(body, %w[])
    time = js['elapsed_time'].to_f
    return time
  end

end
end
