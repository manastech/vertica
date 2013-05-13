require 'test_helper'

class DataRowTest < Test::Unit::TestCase

  def test_initialize
    data = "\x00\n\x00\x00\x00\a1250001\x00\x00\x00\fSouth Africa\xFF\xFF\xFF\xFF\x00\x00\x00\x011\x00\x00\x00\x03238\x00\x00\x00\x132012-02-21 17:26:55\x00\x00\x00\x132012-02-21 17:26:55\x00\x00\x00\b-30.5595\x00\x00\x00\a22.9375\x00\x00\x00\x010"
    row = Vertica::Messages::DataRow.new data
    assert_equal ["1250001", "South Africa", nil, "1", "238", "2012-02-21 17:26:55", "2012-02-21 17:26:55", "-30.5595", "22.9375", "0"], row.values
  end

end
