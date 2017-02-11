# -*- coding: utf-8 -*-
require 'sqlite3'
require 'pry'

module ZipCodeJp
  class DB2

    INSERT_SQL = <<-SQL
      INSERT INTO addresses (
        zip_code,
        prefecture, prefecture_kana, prefecture_code,
        city, city_kana,
        town, town_kana,
        street,
        office_name,
        office_name_kana
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    SQL

    SELECT_SQL = <<-SQL
      SELECT
        prefecture_kana,
        prefecture_code,
        city_kana,
        town_kana
      FROM addresses
      WHERE prefecture = ? AND city = ? AND town = ? LIMIT 1
    SQL

    def initialize
      @db = SQLite3::Database.new('tmp/test.db')
    end

    def retrieve
      @db.execute 'BEGIN TRANSACTION'

      Zip::File.open(open('http://www.post.japanpost.jp/zipcode/dl/jigyosyo/zip/jigyosyo.zip').path) do |archives|
        archives.each do |a|
          CSV.parse(a.get_input_stream.read) do |row|
            h = to_hash(row)

            h[:prefecture_kana],
            h[:prefecture_code],
            h[:city_kana],
            h[:town_kana] = @db.execute(SELECT_SQL, h[:prefecture], h[:city], h[:town]).first

            p h

            @db.execute(
              INSERT_SQL,
              h[:zip_code],
              h[:prefecture], h[:prefecture_kana], h[:prefecture_code],
              h[:city], h[:city_kana],
              h[:town], h[:town_kana],
              h[:street],
              h[:office_name],
              h[:office_name_kana]
            )
          end
        end
      end
      @db.execute 'COMMIT'
    end

    private
    def to_hash(row)
      {
        :zip_code           => row[7],
        :prefecture         => NKF.nkf('-S -w', row[3]),
        :city               => NKF.nkf('-S -w', row[4]),
        :town               => NKF.nkf('-S -w', row[5]),
        :street             => NKF.nkf('-S -w', row[6]),
        :office_name        => NKF.nkf('-S -w', row[2]),
        :office_name_kana   => NKF.nkf('-S -w', row[1])
      }
    end
  end
end
