class TreasureData::API
module AccessControl

  ####
  ## Access Control API
  ##

  # @param [String] subject
  # @param [String] action
  # @param [String] scope
  # @param [Array] grant_option
  # @return [true]
  def grant_access_control(subject, action, scope, grant_option)
    params = {'subject'=>subject, 'action'=>action, 'scope'=>scope, 'grant_option'=>grant_option.to_s}
    code, body, res = post("/v3/acl/grant", params)
    if code != "200"
      raise_error("Granting access control failed", res)
    end
    return true
  end

  # @param [String] subject
  # @param [String] action
  # @param [String] scope
  # @return [true]
  def revoke_access_control(subject, action, scope)
    params = {'subject'=>subject, 'action'=>action, 'scope'=>scope}
    code, body, res = post("/v3/acl/revoke", params)
    if code != "200"
      raise_error("Revoking access control failed", res)
    end
    return true
  end

  # @param [String] user
  # @param [String] action
  # @param [String] scope
  # @return [Array]
  def test_access_control(user, action, scope)
    params = {'user'=>user, 'action'=>action, 'scope'=>scope}
    code, body, res = get("/v3/acl/test", params)
    if code != "200"
      raise_error("Testing access control failed", res)
    end
    js = checked_json(body, %w[permission access_controls])
    perm = js["permission"]
    acl = js["access_controls"].map {|roleinfo|
      subject = roleinfo['subject']
      action = roleinfo['action']
      scope = roleinfo['scope']
      [name, action, scope]
    }
    return perm, acl
  end

  # @return [Array]
  def list_access_controls
    code, body, res = get("/v3/acl/list")
    if code != "200"
      raise_error("Listing access control failed", res)
    end
    js = checked_json(body, %w[access_controls])
    acl = js["access_controls"].map {|roleinfo|
      subject = roleinfo['subject']
      action = roleinfo['action']
      scope = roleinfo['scope']
      grant_option = roleinfo['grant_option']
      [subject, action, scope, grant_option]
    }
    return acl
  end

end
end
