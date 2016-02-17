class TreasureData::API
module Schedule

  ####
  ## Schedule API
  ##

  # @param [String] name
  # @param [Hash] opts
  # @return [String]
  def create_schedule(name, opts)
    params = opts.update({:type=> opts[:type] || opts['type'] || 'hive'})
    code, body, res = post("/v3/schedule/create/#{e name}", params)
    if code != "200"
      raise_error("Create schedule failed", res)
    end
    js = checked_json(body)
    return js['start']
  end

  # @param [String] name
  # @return [Array]
  def delete_schedule(name)
    code, body, res = post("/v3/schedule/delete/#{e name}")
    if code != "200"
      raise_error("Delete schedule failed", res)
    end
    js = checked_json(body, %w[])
    return js['cron'], js["query"]
  end

  # @return [Array]
  def list_schedules
    code, body, res = get("/v3/schedule/list")
    if code != "200"
      raise_error("List schedules failed", res)
    end
    js = checked_json(body, %w[schedules])
    result = []
    js['schedules'].each {|m|
      name = m['name']
      cron = m['cron']
      query = m['query']
      database = m['database']
      result_url = m['result']
      timezone = m['timezone']
      delay = m['delay']
      next_time = m['next_time']
      priority = m['priority']
      retry_limit = m['retry_limit']
      result << [name, cron, query, database, result_url, timezone, delay, next_time, priority, retry_limit, nil] # same as database
    }
    return result
  end

  # @param [String] name
  # @param [Hash] params
  # @return [nil]
  def update_schedule(name, params)
    code, body, res = post("/v3/schedule/update/#{e name}", params)
    if code != "200"
      raise_error("Update schedule failed", res)
    end
    return nil
  end

  # @param [String] name
  # @param [Fixnum] from
  # @param [Fixnum] to
  # @return [Array]
  def history(name, from=0, to=nil)
    params = {}
    params['from'] = from.to_s if from
    params['to'] = to.to_s if to
    code, body, res = get("/v3/schedule/history/#{e name}", params)
    if code != "200"
      raise_error("List history failed", res)
    end
    js = checked_json(body, %w[history])
    result = []
    js['history'].each {|m|
      job_id = m['job_id']
      type = (m['type'] || '?').to_sym
      database = m['database']
      status = m['status']
      query = m['query']
      start_at = m['start_at']
      end_at = m['end_at']
      scheduled_at = m['scheduled_at']
      result_url = m['result']
      priority = m['priority']
      result << [scheduled_at, job_id, type, status, query, start_at, end_at, result_url, priority, database]
    }
    return result
  end

  # @param [String] name
  # @param [String] time
  # @param [Fixnum] num
  # @return [Array]
  def run_schedule(name, time, num)
    params = {}
    params = {'num' => num} if num
    code, body, res = post("/v3/schedule/run/#{e name}/#{e time}", params)
    if code != "200"
      raise_error("Run schedule failed", res)
    end
    js = checked_json(body, %w[jobs])
    result = []
    js['jobs'].each {|m|
      job_id = m['job_id']
      scheduled_at = m['scheduled_at']
      type = (m['type'] || '?').to_sym
      result << [job_id, type, scheduled_at]
    }
    return result
  end

end
end
