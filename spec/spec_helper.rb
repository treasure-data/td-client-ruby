begin
  require 'simplecov'
  SimpleCov.start
rescue LoadError
end

$LOAD_PATH << File.dirname(__FILE__)+"../lib"
require 'td-client'

include TreasureData

