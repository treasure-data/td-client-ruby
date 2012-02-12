require 'spec_helper'

describe API do
  it 'normalize_database_name should return normalized data' do
    API.normalize_database_name('ab0_').should == 'ab0_'
    API.normalize_database_name('a').should == 'a__'
    API.normalize_database_name('a'*33).should == 'a'*30+'__'
    API.normalize_database_name('abcD').should == 'abcd'
    API.normalize_database_name('a-b*').should == 'a_b_'
  end
end

