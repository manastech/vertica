class Vertica::Portal

  attr_reader :connection, :name, :row_style, :row_handler, :result, :tag

  def initialize(connection, row_description, row_style = :hash, &row_handler)
    @connection, @row_style, @name = connection, row_style, ""
    @row_handler = row_handler if block_given?

    @columns = row_description.fields.map { |fd| Vertica::Column.new(fd) }
    @result  = Vertica::Result.new(@columns) if buffer_rows?
  end

  def retreive_rows
    begin
      case message = @connection.read_message
      when Vertica::Messages::DataRow
        record = format_row(message)
        buffer_rows? ? @result.rows << record : @row_handler.call(record)
      when Vertica::Messages::CommandComplete
        @tag = message.tag
      when Vertica::Messages::PortalSuspended
        # noop
      else
        @connection.process_message(message)
      end
    end until message.kind_of?(Vertica::Messages::CommandComplete) ||
              message.kind_of?(Vertica::Messages::PortalSuspended)

    return @result
  end

  def buffer_rows?
    @row_handler.nil?
  end

  def format_row(data_row_message)
    send("format_row_as_#{@row_style}", data_row_message)
  end

  def format_row_as_hash(data_row_message)
    row = {}
    data_row_message.values.each_with_index do |value, idx|
      col = @columns[idx]
      row[col.name] = col.convert(value)
    end
    return row
  end

  def format_row_as_array(data_row_message)
    row = []
    data_row_message.values.each_with_index do |value, idx|
      row << @columns[idx].convert(value)
    end
    return row
  end

  def close
    @connection.write Vertica::Messages::Close.new(:portal, @name)
    @connection.write Vertica::Messages::Sync.new
    begin
      message = @connection.read_message
      @connection.process_message(message) unless message.instance_of?(Vertica::Messages::CloseComplete)
    end until Vertica::Messages::ReadyForQuery === message
  end
end
