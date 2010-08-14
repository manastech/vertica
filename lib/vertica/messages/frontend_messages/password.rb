
module Vertica
  module Messages
    class Password < FrontendMessage
      message_id ?p

      def initialize(password, auth_method = nil, options = {})
        @password = password
        @auth_method = auth_method || Messages::Authentication::CLEARTEXT_PASSWORD
        @options = options
      end

      def password
        case @auth_method
        when Authentication::CLEARTEXT_PASSWORD
          @password
        when Authentication::CRYPT_PASSWORD
          @password.crypt(options[:salt])
        when Authentication::MD5_PASSWORD
          require 'digest/md5'
          @password = Digest::MD5.hexdigest(@password + @options[:user])
          @password = Digest::MD5.hexdigest(m + @options[:salt])
          @password = 'md5' + @password
        else
          raise ArgumentError.new("unsupported authentication method: #{@auth_method}")
        end
      end

      def to_bytes
        size = LENGTH_SIZE
        size += @password.length + 1
        [ message_id.to_byte,
          size.to_network_int32,
          password.to_cstring
        ].join
      end

    end
  end
end