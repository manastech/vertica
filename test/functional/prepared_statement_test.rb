require 'test_helper'

class PreparedStatementTest < Test::Unit::TestCase

  def setup
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    @connection.query("CREATE TABLE IF NOT EXISTS test_ruby_vertica_table (id int, name varchar(100))")
    @connection.query("INSERT INTO test_ruby_vertica_table VALUES (1, 'matt')")
    @connection.query("COMMIT")
  end

  def teardown
    @connection.query("DROP TABLE IF EXISTS test_ruby_vertica_table CASCADE")
    @connection.close
  end

  def test_unbuffered_prepared_statement_without_parameters
    row_count = 0
    @connection.prepare("SELECT * FROM test_ruby_vertica_table WHERE id = 1") do |statement|
      statement.execute { |row| row_count += 1 }
    end

    assert_equal row_count, 1
  end

  def test_buffered_prepared_statement_without_parameters
    statement = @connection.prepare("SELECT * FROM test_ruby_vertica_table WHERE id = 1")
    assert_equal [{:id => 1, :name => "matt"}], statement.execute.rows
    statement.close
  end

  def test_resultless_query_without_parameters
    statement = @connection.prepare("CREATE TABLE IF NOT EXISTS test_ruby_vertica_table2 (id int, name varchar(100))")
    assert_nil statement.execute
  end

  # def test_unbuffered_prepared_statement_with_single_parameter
  #   @connection.debug = true
  #   row_count = 0
  #   @connection.prepare("SELECT * FROM test_ruby_vertica_table WHERE id = $1", 1) do |statement|
  #     statement.execute(1) { |row| row_count += 1 }
  #   end
  #
  #   assert_equal row_count, 1
  #   @connection.debug = false
  # end
end
