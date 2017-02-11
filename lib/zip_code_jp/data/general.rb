# -*- coding: utf-8 -*-
require 'zip'
require 'nokogiri'
# require 'open-uri'
require 'csv'
require 'nkf'
require 'yaml'
require 'zip_code_jp'
require 'zip_code_jp/data/base'

module ZipCodeJp
  module Data
    class General < Base

      private

      ZIP_URL_DOMAIN = 'http://zipcloud.ibsnet.co.jp'

      def to_hash(row)
        {
          zip_code:        row[2],
          prefecture:      NKF.nkf('-S -w', row[6]),
          prefecture_kana: NKF.nkf('-S -w', row[3]),
          city:            NKF.nkf('-S -w', row[7]),
          city_kana:       NKF.nkf('-S -w', row[4]),
          town:            NKF.nkf('-S -w', row[8]),
          town_kana:       NKF.nkf('-S -w', row[5])
        }
      end

      def retrieve
        arr = []
        Zip::File.open(open(zip_url).path) do |archives|
          archives.each do |a|
            CSV.parse(a.get_input_stream.read) do |row|
              h = to_hash(row)
              h[:prefecture_code] = prefecture_codes.invert[h[:prefecture]]
              arr << h
            end
          end
        end
        arr
      end

      def prefecture_codes
        @prefecture_codes ||= YAML.load(File.open("#{ZipCodeJp::DATA_DIR}/prefecture_code.yml"))
      end

      def zip_url
        html = Nokogiri::HTML(open(ZIP_URL_DOMAIN))
        url = html.css('[href^="/zipcodedata/download"]').last.attributes['href']
        "#{ZIP_URL_DOMAIN}#{url}"
      end
    end
  end
end
