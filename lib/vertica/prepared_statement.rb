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
      when Vertica::Messages::RowDescription
        @row_description = message
      when Vertica::Messages::NoData
        # noop
      else
        @connection.process_message(message)
      end
    end until Vertica::Messages::ReadyForQuery === message

    raise Vertica::Error::QueryError, error if error
    return self
  end
  
  def bind(*parameters)
    options      = parameters.last.kind_of?(Hash) ? parameters.pop : {}
    @portal_name = options[:portal] || ""
    @connection.write Vertica::Messages::Bind.new(@portal_name, @name, parameters.map { |p| p.to_s })
    return self
  end

  def execute(*parameters, &block)
    max_rows = parameters.last.kind_of?(Hash) ? parameters.last[:max_rows] : 0
    bind(*parameters) if @portal_name.nil?
    @connection.write Vertica::Messages::Execute.new(@portal_name, max_rows)
    @connection.write Vertica::Messages::Sync.new

    result, error = nil
    begin
      case message = @connection.read_message
      when Vertica::Messages::ErrorResponse
        error = message.error_message
      when Vertica::Messages::BindComplete
        if @row_description
          portal = Vertica::Portal.new(@connection, @row_description, row_style, &block)
          result = portal.retreive_rows
        else
          wait_for_command_complete
        end
      else
        @connection.process_message(message)
      end
    end until Vertica::Messages::ReadyForQuery === message

    raise Vertica::Error::QueryError, error if error
    return result
  end

  def wait_for_command_complete
    until Vertica::Messages::CommandComplete === (message = @connection.read_message)
      @connection.process_message(message)
    end
  end
  
  def close_portal
    @connection.write Vertica::Messages::Close.new(:portal, @portal_name)
    @connection.write Vertica::Messages::Sync.new
    begin
      message = @connection.read_message
      @connection.process_message(message) unless message.instance_of?(Vertica::Messages::CloseComplete)
    end until Vertica::Messages::ReadyForQuery === message
    @portal_name = nil
  end

  def close
    @connection.write Vertica::Messages::Close.new(:prepared_statement, @name)
    @connection.write Vertica::Messages::Sync.new
    begin
      message = @connection.read_message
      @connection.process_message(message) unless message.instance_of?(Vertica::Messages::CloseComplete)
    end until Vertica::Messages::ReadyForQuery === message
    @portal_name, @name = nil, nil
  end
end
