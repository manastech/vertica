class Vertica::PreparedStatement

  attr_reader :sql, :name, :param_types, :parameter_description, :row_description, :row_style

  def initialize(connection, sql, param_count = 0, options = {})
    @connection, @sql = connection, sql
    @name         = options[:name] || ""
    @param_types  = Array.new(param_count, 0)
    @row_style    = options[:row_style] || @connection.row_style || :hash
  end

  def prepare
    @connection.write Vertica::Messages::Parse.new(@name, @sql, @param_types)
    @connection.write Vertica::Messages::Describe.new(:prepared_statement, @name)
    @connection.write Vertica::Messages::Sync.new

    error = nil
    begin
      case message = @connection.read_message
      when Vertica::Messages::ErrorResponse
        error = message.error_message
      when Vertica::Messages::ParseComplete
        # all ok, get description messages
      when Vertica::Messages::ParameterDescription
        @parameter_description = message
      when Vertica::Messages::RowDescription, Vertica::Messages::NoData
        @row_description = message
      else
        @connection.process_message(message)
      end
    end until Vertica::Messages::ReadyForQuery === message

    raise Vertica::Error::QueryError, error if error
    return self
  end

  def execute(*parameters, &block)
    options     = parameters.last.kind_of?(Hash) ? parameters.pop : {}
    portal_name = options[:portal]   || ""
    max_rows    = options[:max_rows] || 0

    @connection.write Vertica::Messages::Bind.new(portal_name, @name, parameters.map { |p| p.to_s })
    @connection.write Vertica::Messages::Execute.new(portal_name, max_rows)
    @connection.write Vertica::Messages::Sync.new

    result, error = nil
    begin
      case message = @connection.read_message
      when Vertica::Messages::ErrorResponse
        error = message.error_message
      when Vertica::Messages::BindComplete
        portal = Vertica::Portal.new(@connection, @row_description, row_style, &block)
        result = portal.retreive_rows
        portal.close
      else
        @connection.process_message(message)
      end
    end until Vertica::Messages::ReadyForQuery === message

    raise Vertica::Error::QueryError, error if error
    return result
  end

  def close
    @connection.write Vertica::Messages::Close.new(:prepared_statement, @name)
    @connection.write Vertica::Messages::Sync.new
    until Vertica::Messages::CloseComplete === (message = @connection.read_message)
      @connection.process_message(message)
    end
  end
end
