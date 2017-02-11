# -*- coding: utf-8 -*-
require 'json'
require 'sqlite3'
require 'yaml'
require 'pry'

module ZipCodeJp
  class Export2
    SELECT_SQL = <<-SQL
      SELECT
        zip_code,
        prefecture, prefecture_kana, prefecture_code,
        city, city_kana,
        town, town_kana,
        street,
        office_name,
        office_name_kana
      FROM addresses
      ORDER BY zip_code
    SQL

    def execute
      zip_codes().each do |prefix, value|
        file_path = "#{ZipCodeJp::DATA_DIR}/zip_code/#{prefix}.json"
        File.open(file_path, 'wb') do |file|
          file.write JSON.pretty_generate(value)
        end
      end
    end

    private
    def zip_codes
      @db = SQLite3::Database.new('tmp/test.db')
      zip_codes = {}

      @db.execute(SELECT_SQL) do |row|
        h = {}
        h[:zip_code],
        h[:prefecture],
        h[:prefecture_kana],
        h[:prefecture_code],
        h[:city],
        h[:city_kana],
        h[:town],
        h[:town_kana],
        h[:street],
        h[:office_name],
        h[:office_name_kana] = row

        first_prefix  = h[:zip_code].slice(0, 3)
        second_prefix = h[:zip_code].slice(3, 4)
        zip_codes[first_prefix] = {} unless zip_codes[first_prefix]

        if zip_codes[first_prefix][second_prefix] && !zip_codes[first_prefix][second_prefix].instance_of?(Array)
          zip_codes[first_prefix][second_prefix] = [zip_codes[first_prefix][second_prefix]]
        end

        if zip_codes[first_prefix][second_prefix].instance_of?(Array)
          zip_codes[first_prefix][second_prefix].push h
        else
          zip_codes[first_prefix] = zip_codes[first_prefix].merge({second_prefix => h})
        end
      end

      zip_codes
    end
  end
end
