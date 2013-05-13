module Vertica
  module Messages
    class DataRow < BackendMessage
      message_id 'D'

      attr_reader :values

      def initialize(data)
        @values = Vertica.parse_data_row(data)
      end
    end
  end
end
