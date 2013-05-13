require 'test_helper'

class VerticaTest < Test::Unit::TestCase

  def test_parse_date
    assert_equal Vertica.parse_date("2012-01-02"), Date.new(2012, 1, 2)
  end

  def test_parse_timestamp
    string = "2010-01-01 12:00:00"
    assert_equal DateTime.parse(string, true), Vertica.parse_timestamp(string)
  end

  def test_parse_timestamp_tz
    string = "2013-05-10 05:17:46.051257-04"
    assert_equal DateTime.new(2013, 5, 10, 5, 17, 46.051257, -4), Vertica.parse_timestamp_tz(string)
  end

end
