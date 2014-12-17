module TreasureData

class ParameterValidationError < StandardError
end

# Generic API error
class APIError < StandardError
end

# 401 API errors
class AuthError < APIError
end

# 403 API errors, used for database permissions
class ForbiddenError < APIError
end

# 409 API errors
class AlreadyExistsError < APIError
end

# 404 API errors
class NotFoundError < APIError
end

end
