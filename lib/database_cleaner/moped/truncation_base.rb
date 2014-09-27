require 'database_cleaner/moped/base'
require 'database_cleaner/generic/truncation'

module DatabaseCleaner
  module Moped
    module TruncationBase
      include ::DatabaseCleaner::Moped::Base
      include ::DatabaseCleaner::Generic::Truncation

      def clean
        if @only
          collections.each do |s, c|
              s[c].find.remove_all if @only.include?(c)
          end
        else
          collections.each do |s, c|
            s[c].find.remove_all unless @tables_to_exclude.include?(c)
          end
        end
        true
      end

      private

      def collections
        if db != :default
          session.use(db)
        end

        result = []
        session.each do |s|
          s['system.namespaces'].find(:name => { '$not' => /\.system\.|\$/ }).to_a.each do |collection|
            _, name = collection['name'].split('.', 2)
            result << [s, name]
          end
        end
        result
      end

    end
  end
end
