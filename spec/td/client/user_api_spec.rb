require 'spec_helper'
require 'td/client/spec_resources'
require 'json'

describe 'User API' do
  include_context 'spec symbols'
  include_context 'common helper'

  let :api do
    API.new(nil)
  end

  describe 'authenticate' do
    it 'returns apikey' do
      stub_api_request(:post, "/v3/user/authenticate").
        to_return(:body => {'apikey' => 'apikey'}.to_json)
      api.authenticate('user', 'password').should == 'apikey'
    end

    it 'raises AuthError for authentication failure' do
      stub_api_request(:post, "/v3/user/authenticate").
        to_return(:status => 400, :body => {'apikey' => 'apikey'}.to_json)
      expect {
        api.authenticate('user', 'password')
      }.to raise_error(TreasureData::AuthError)
    end

    it 'raises APIError for other error' do
      stub_api_request(:post, "/v3/user/authenticate").
        to_return(:status => 500, :body => {'apikey' => 'apikey'}.to_json)
      expect {
        api.authenticate('user', 'password')
      }.to raise_error(TreasureData::APIError)
    end
  end

  describe 'list_users' do
    it 'returns users' do
      stub_api_request(:get, "/v3/user/list").
        to_return(:body => {'users' => [{'name' => 'name1', 'email' => 'email1'}, {'name' => 'name2', 'email' => 'email2'}]}.to_json)
      api.list_users.should == [
        ['name1', nil, nil, 'email1'],
        ['name2', nil, nil, 'email2'],
      ]
    end
  end

  describe 'add_user' do
    it 'runs' do
      stub_api_request(:post, "/v3/user/add/name").to_return(:body => {}.to_json)
      api.add_user('name', "org", 'name+suffix@example.com', 'password').should == true
    end

    # TODO
    it 'does not escape sp but it must be a bug' do
      stub_api_request(:post, "/v3/user/add/!%20%20%20%20@%23$%25%5E&*()_%2B%7C~%2Ecom").to_return(:body => {}.to_json)
      api.add_user('!    @#$%^&*()_+|~.com', "org", 'name+suffix@example.com', 'password').should == true
    end
  end

  describe 'remove_user' do
    it 'runs' do
      stub_api_request(:post, "/v3/user/remove/name").to_return(:body => {}.to_json)
      api.remove_user('name').should == true
    end
  end

  describe 'change_email' do
    it 'runs' do
      stub_api_request(:post, "/v3/user/email/change/name").
        with(:body => {'email' => 'new@email.com'}).
        to_return(:body => {}.to_json)
      api.change_email('name', 'new@email.com').should == true
    end
  end

  describe 'list_apikeys' do
    it 'runs' do
      stub_api_request(:get, "/v3/user/apikey/list/name").
        to_return(:body => {'apikeys' => ['key1', 'key2']}.to_json)
      api.list_apikeys('name').should == ['key1', 'key2']
    end
  end

  describe 'add_apikey' do
    it 'does not return the generated apikey because you can list apikey afterwards' do
      stub_api_request(:post, "/v3/user/apikey/add/name").
        to_return(:body => {'apikey' => 'apikey'}.to_json)
      api.add_apikey('name').should == true
    end
  end

  describe 'remove_apikey' do
    it 'runs' do
      stub_api_request(:post, "/v3/user/apikey/remove/name").
        to_return(:body => {}.to_json)
      api.remove_apikey('name', 'apikey').should == true
    end
  end

  describe 'change password' do
    it 'runs' do
      stub_api_request(:post, "/v3/user/password/change/name").
        with(:body => {'password' => 'password'}).
        to_return(:body => {}.to_json)
      api.change_password('name', 'password').should == true
    end
  end

  describe 'change my password' do
    it 'runs' do
      stub_api_request(:post, "/v3/user/password/change").
        with(:body => {'old_password' => 'old_password', 'password' => 'password'}).
        to_return(:body => {}.to_json)
      api.change_my_password('old_password', 'password').should == true
    end
  end
end
