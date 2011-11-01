module Vertica
  module Messages
    class Parse < FrontendMessage
      message_id 'P'

      def initialize(name, sql, param_types)
        @name, @sql, @param_types = name, sql, param_types
      end

      def to_bytes
        message_string([@name, @sql, @param_types.length, *@param_types].pack('Z*Z*nN*'))
      end
    end
  end
end
