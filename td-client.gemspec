# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)
require 'td/client/version'

Gem::Specification.new do |gem|
  gem.name          = "td-client"
  gem.summary       = "Treasure Data API library for Ruby"
  gem.description   = "Treasure Data API library for Ruby"
  gem.authors       = ["Treasure Data, Inc."]
  gem.email         = "support@treasure-data.com"
  gem.homepage      = "http://treasuredata.com/"
  gem.version       = TreasureData::Client::VERSION
  gem.has_rdoc      = false
  gem.test_files    = Dir["spec/**/*_spec.rb"]
  gem.files         = Dir["lib/**/*", "ext/**/*", "data/**/*", "spec/**/*.rb"]
  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ["lib"]
  gem.required_ruby_version = '>= 2.1'

  gem.add_dependency "msgpack", ">= 0.6"
  gem.add_dependency "json", ">= 1.7.6"
  gem.add_dependency "httpclient", ">= 2.7"
  gem.add_development_dependency "rspec", "~> 3.0"
  gem.add_development_dependency 'coveralls'
  gem.add_development_dependency "webmock", "~> 1.16"
  gem.add_development_dependency 'simplecov', '>= 0.5.4'
  gem.add_development_dependency 'rake'
  gem.add_development_dependency 'yard'
end
