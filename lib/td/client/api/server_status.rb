class TreasureData::API
module ServerStatus

  ####
  ## Server Status API
  ##

  # => status:String
  # @return [String] HTTP status code
  def server_status
    code, body, res = get('/v3/system/server_status')
    if code != "200"
      return "Server is down (#{code})"
    end
    js = checked_json(body, %w[status])
    status = js['status']
    return status
  end

end
end
