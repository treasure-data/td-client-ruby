require 'openssl'
module OpenSSL
  module SSL
    class SSLContext

      # For disabling SSLv3 connection in favor of POODLE Attack protection
      #
      # Allow 'options' customize through Thread local storage since
      # Net::HTTP does not support 'options' configuration.
      #
      alias original_set_params set_params
      def set_params(params={})
        original_set_params(params)
        self.options |= OP_NO_SSLv3 if Thread.current[:SET_SSL_OP_NO_SSLv3]
      end
    end
  end
end
