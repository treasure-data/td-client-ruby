module TreasureData

class ParameterValidationError < StandardError
end

# Generic API error
class APIError < StandardError
  attr_reader :api_backtrace

  def initialize(error_message = nil, api_backtrace = nil)
    super(error_message)
    @api_backtrace = api_backtrace == '' ? nil : api_backtrace
  end
end

# 4xx Client Errors
class ClientError < APIError
end

# 400 Bad Request
class BadRequestError < ClientError
end

# 401 Unauthorized
class AuthError < ClientError
end

# 403 Forbidden, used for database permissions
class ForbiddenError < ClientError
end

# 404 Not Found
class NotFoundError < ClientError
end

# 405 Method Not Allowed
class MethodNotAllowedError < ClientError
end

# 409 Conflict
class AlreadyExistsError < ClientError
  attr_reader :conflicts_with
  def initialize(error_message = nil, api_backtrace = nil, conflicts_with=nil)
    super(error_message, api_backtrace)
    @conflicts_with = conflicts_with
  end
end

# 415 Unsupported Media Type
class UnsupportedMediaTypeError < ClientError
end

# 422 Unprocessable Entity
class UnprocessableEntityError < ClientError
end

# 429 Too Many Requests
class TooManyRequestsError < ClientError
end

end
