# -*- coding: utf-8 -*-
require 'sqlite3'

module ZipCodeJp
  class DB
    ZIP_URL_DOMAIN  = 'http://zipcloud.ibsnet.co.jp'

    def initialize
      @db = SQLite3::Database.new('tmp/test.db')
    end

    def cleanup
      @db.execute <<-SQL
        DROP TABLE IF EXISTS addresses;
      SQL

      @db.execute <<-SQL
        CREATE TABLE addresses (
          zip_code VARCHAR(7),
          prefecture VARCHAR(256),
          prefecture_kana VARCHAR(256),
          prefecture_code VARCHAR(2),
          city VARCHAR(256),
          city_kana VARCHAR(256),
          town VARCHAR(256),
          town_kana VARCHAR(256),
          street VARCHAR(256),
          office_name VARCHAR(256),
          office_name_kana VARCHAR(256)
        );
      SQL
    end

    def retrieve
      query = <<-SQL
        INSERT INTO addresses (
          zip_code,
          prefecture, prefecture_kana, prefecture_code,
          city, city_kana,
          town, town_kana
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
      SQL

      @db.execute 'BEGIN TRANSACTION'
      Zip::File.open(open(zip_url).path) do |archives|
        archives.each do |a|
          CSV.parse(a.get_input_stream.read) do |row|
            h = to_hash(row)
            h[:prefecture_code] = prefecture_codes.invert[h[:prefecture]]

            # p h
            @db.execute(
              query,
              h[:zip_code],
              h[:prefecture], h[:prefecture_kana], h[:prefecture_code],
              h[:city], h[:city_kana],
              h[:town], h[:town_kana]
            )
          end
        end
      end
      @db.execute 'COMMIT'
    end

    private

    def prefecture_codes
      prefecture_codes ||= YAML.load(File.open("#{ZipCodeJp::DATA_DIR}/prefecture_code.yml"))
    end

    def to_hash(row)
      {
        :zip_code        => row[2],
        :prefecture      => NKF.nkf('-S -w', row[6]),
        :prefecture_kana => NKF.nkf('-S -w', row[3]),
        :city            => NKF.nkf('-S -w', row[7]),
        :city_kana       => NKF.nkf('-S -w', row[4]),
        :town            => NKF.nkf('-S -w', row[8]),
        :town_kana       => NKF.nkf('-S -w', row[5])
      }
    end

    def zip_url
      html = Nokogiri::HTML(open(ZIP_URL_DOMAIN))
      url = html.css('[href^="/zipcodedata/download"]').last.attributes['href']
      "#{ZIP_URL_DOMAIN}#{url}"
    end
  end
end
