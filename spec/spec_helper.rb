require 'rubygems'

begin
  require 'simplecov'
  SimpleCov.start
rescue LoadError
end

require 'rspec'
require 'webmock/rspec'

include WebMock::API

$LOAD_PATH << File.dirname(__FILE__)+"../lib"
require 'td-client'

include TreasureData

def stub_api_request(method, path)
  stub_request(method, "https://api.treasure-data.com#{path}")
end
