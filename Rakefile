require 'bundler/gem_tasks'
require 'zip_code_jp/data/general'
require 'zip_code_jp/data/office'
require 'zip_code_jp/data/exporter'

namespace :zip_code_jp do
  desc 'Refresh zip code data'
  task :refresh do
    puts 'Reset temp DB...'
    general = ZipCodeJp::Data::General.new
    general.reset_table

    puts 'Retrieveing general zip data...'
    general.retrieve_and_save

    puts 'Retrieveing office zip data...'
    office = ZipCodeJp::Data::Office.new
    office.retrieve_and_save

    puts 'Extract to json files...'
    ZipCodeJp::Data::Exporter.new.execute
  end
end
