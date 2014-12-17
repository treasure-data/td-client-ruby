class TreasureData::API
module Account

  ####
  ## Account API
  ##

  def show_account
    code, body, res = get("/v3/account/show")
    if code != "200"
      raise_error("Show account failed", res)
    end
    js = checked_json(body, %w[account])
    a = js["account"]
    account_id = a['id'].to_i
    plan = a['plan'].to_i
    storage_size = a['storage_size'].to_i
    guaranteed_cores = a['guaranteed_cores'].to_i
    maximum_cores = a['maximum_cores'].to_i
    created_at = a['created_at']
    return [account_id, plan, storage_size, guaranteed_cores, maximum_cores, created_at]
  end

  def account_core_utilization(from, to)
    params = { }
    params['from'] = from.to_s if from
    params['to'] = to.to_s if to
    code, body, res = get("/v3/account/core_utilization", params)
    if code != "200"
      raise_error("Show account failed", res)
    end
    js = checked_json(body, %w[from to interval history])
    from = Time.parse(js['from']).utc
    to = Time.parse(js['to']).utc
    interval = js['interval'].to_i
    history = js['history']
    return [from, to, interval, history]
  end

end
end
