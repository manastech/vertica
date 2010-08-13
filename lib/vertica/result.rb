require 'tempfile'
require 'json'

module Vertica
  class Result

    def initialize
      @field_values = Tempfile.new('vertica-result')
      @field_descriptions = []
    end

    def add_description(description)
      @field_descriptions = description
    end

    def add_value(value)
      @field_values.puts value.to_json
    end

    def row_count
      @row_count ||= @field_values.length
    end

    def columns
      @columns ||= @field_descriptions.map { |fd| Column.new(fd[:type_modifier], fd[:format_code], fd[:table_oid], fd[:name], fd[:attribute_number], fd[:data_type_oid], fd[:data_type_size]) }
    end

    def each(&block)
      @field_values.rewind
      @field_values.each do |line|
        result_cols = JSON.parse(line)
        row = []
        result_cols.each_with_index { |col, idx| row << self.columns[idx].convert(col['value']) }
        block.call row
      end
    end
  end
end
