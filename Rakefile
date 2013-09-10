require 'rake'
require 'rake/testtask'
require 'rake/clean'
require 'rspec/core/rake_task'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "td-client"
    gemspec.summary = "Treasure Data API library for Ruby"
    gemspec.authors = ["Sadayuki Furuhashi"]
    #gem.email       = "support@treasure-data.com"
    #gem.homepage    = "http://treasure-data.com/"
    gemspec.has_rdoc = false
    gemspec.require_paths = ["lib"]
    gemspec.add_dependency "msgpack", [">= 0.4.4", "!= 0.5.0", "!= 0.5.1", "!= 0.5.2", "!= 0.5.3", "< 0.6.0"]
    gemspec.add_dependency "json", ">= 1.7.6"
    gemspec.add_dependency "httpclient", "~> 2.3.4"
    gemspec.add_development_dependency "rspec", "~> 2.8.0"
    gemspec.add_development_dependency "webmock", "~> 1.9.0"
    gemspec.test_files = Dir["spec/**/*_spec.rb"]
    gemspec.files = Dir["lib/**/*", "ext/**/*", "data/**/*", "spec/**/*.rb"]
    gemspec.executables = []
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end

VERSION_FILE = "lib/td/client/version.rb"

file VERSION_FILE => ["VERSION"] do |t|
  version = File.read("VERSION").strip
  File.open(VERSION_FILE, "w") {|f|
    f.write <<EOF
module TreasureData
  class Client
VERSION = '#{version}'
  end
end
EOF
  }
end

RSpec::Core::RakeTask.new(:spec)

task :test  => :spec

task :default => [VERSION_FILE, :build]

