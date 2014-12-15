class TreasureData::API
module User

  ####
  ## User API
  ##

  # apikey:String
  def authenticate(user, password)
    code, body, res = post("/v3/user/authenticate", {'user'=>user, 'password'=>password})
    if code != "200"
      if code == "400"
        raise_error("Authentication failed", res, AuthError)
      else
        raise_error("Authentication failed", res)
      end
    end
    js = checked_json(body, %w[apikey])
    apikey = js['apikey']
    return apikey
  end

  # => [[name:String,organization:String,[user:String]]
  def list_users
    code, body, res = get("/v3/user/list")
    if code != "200"
      raise_error("List users failed", res)
    end
    js = checked_json(body, %w[users])
    result = js["users"].map {|roleinfo|
      name = roleinfo['name']
      email = roleinfo['email']
      [name, nil, nil, email] # set nil to org and role for API compatibility
    }
    return result
  end

  # => true
  def add_user(name, org, email, password)
    params = {'organization'=>org, :email=>email, :password=>password}
    code, body, res = post("/v3/user/add/#{e name}", params)
    if code != "200"
      raise_error("Adding user failed", res)
    end
    return true
  end

  # => true
  def remove_user(user)
    code, body, res = post("/v3/user/remove/#{e user}")
    if code != "200"
      raise_error("Removing user failed", res)
    end
    return true
  end

  # => true
  def change_email(user, email)
    params = {'email' => email}
    code, body, res = post("/v3/user/email/change/#{e user}", params)
    if code != "200"
      raise_error("Changing email failed", res)
    end
    return true
  end

  # => [apikey:String]
  def list_apikeys(user)
    code, body, res = get("/v3/user/apikey/list/#{e user}")
    if code != "200"
      raise_error("List API keys failed", res)
    end
    js = checked_json(body, %w[apikeys])
    return js['apikeys']
  end

  # => true
  def add_apikey(user)
    code, body, res = post("/v3/user/apikey/add/#{e user}")
    if code != "200"
      raise_error("Adding API key failed", res)
    end
    return true
  end

  # => true
  def remove_apikey(user, apikey)
    params = {'apikey' => apikey}
    code, body, res = post("/v3/user/apikey/remove/#{e user}", params)
    if code != "200"
      raise_error("Removing API key failed", res)
    end
    return true
  end

  # => true
  def change_password(user, password)
    params = {'password' => password}
    code, body, res = post("/v3/user/password/change/#{e user}", params)
    if code != "200"
      raise_error("Changing password failed", res)
    end
    return true
  end

  # => true
  def change_my_password(old_password, password)
    params = {'old_password' => old_password, 'password' => password}
    code, body, res = post("/v3/user/password/change", params)
    if code != "200"
      raise_error("Changing password failed", res)
    end
    return true
  end

end
end
