class Vertica::Query

  attr_reader :connection, :sql, :row_style, :tag
  attr_accessor :row_handler, :copy_handler

  def initialize(connection, sql, options = {})
    @connection, @sql = connection, sql
    
    @row_style    = options[:row_style] || @connection.row_style || :hash
    @row_handler  = options[:row_handler] 
    @copy_handler = options[:copy_handler]
  end
  
  
  def run
    @connection.write Vertica::Messages::Query.new(@sql)
    result, error = nil, nil
    begin
      case message = @connection.read_message
      when Vertica::Messages::ErrorResponse
        error = message.error_message
      when Vertica::Messages::EmptyQueryResponse
        error = "The provided query was empty."
      when Vertica::Messages::CopyInResponse
        handle_copy_from_stdin
      when Vertica::Messages::CommandComplete
        @tag = message.tag
      when Vertica::Messages::RowDescription
        result = Vertica::Portal.new(@connection, message, row_style, &row_handler).retreive_rows
      else
        @connection.process_message(message)
      end
    end until message.kind_of?(Vertica::Messages::ReadyForQuery)
    
    raise Vertica::Error::QueryError, error unless error.nil?
    return result
  end
  
  def write(data)
    @connection.write Vertica::Messages::CopyData.new(data)
    return self
  end
  
  alias_method :<<, :write
  
  protected
  
  def handle_copy_from_stdin
    if copy_handler.nil?
      @connection.write Vertica::Messages::CopyFail.new('no handler provided')
    else
      begin
        if copy_handler.call(self) == :rollback
          @connection.write Vertica::Messages::CopyFail.new("rollback")
        else
          @connection.write Vertica::Messages::CopyDone.new
        end
      rescue => e
        @connection.write Vertica::Messages::CopyFail.new(e.message)
        raise
      end
    end
  end
end
