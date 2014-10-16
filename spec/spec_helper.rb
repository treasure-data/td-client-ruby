require 'rubygems'

begin
  require 'simplecov'
  SimpleCov.start
rescue LoadError
end

require 'rspec'
require 'webmock/rspec'
WebMock.disable_net_connect!(:allow_localhost => true)

include WebMock::API

$LOAD_PATH << File.dirname(__FILE__)+"../lib"
require 'td-client'
require 'msgpack'
require 'json'

include TreasureData

shared_context 'common helper' do
  let :account_id do
    1
  end

  let :headers do
    if RUBY_VERSION >= "2.0.0"
      {'Accept' => '*/*', 'Accept-Encoding' => /gzip/, 'Date' => /.*/, 'User-Agent' => /Ruby/}
    else
      {'Accept' => '*/*', 'Date' => /.*/, 'User-Agent' => /Ruby/}
    end
  end

  def stub_api_request(method, path, opts = nil)
    scheme = 'http'
    with_opts = {:headers => headers}
    if opts
      scheme = 'https' if opts[:ssl]
      with_opts[:query] = opts[:query] if opts[:query]
    end
    stub_request(method, "#{scheme}://api.treasure-data.com#{path}").with(with_opts)
  end

  def e(s)
    require 'cgi'
    CGI.escape(s.to_s)
  end
end
