require 'spec_helper'

describe API do
  it 'initialize should raise an error with invalid endpoint' do
    expect {
      API.new(nil, :endpoint => 'smtp://api.tester.com:1000')
    }.to raise_error(RuntimeError, /Invalid endpoint:/)
  end

  VALID_NAMES = [
    'abc',
    'abc_cd',
    '_abc_cd',
    '_abc_',
    'ab0_',
    'ab0',
  ]

  INVALID_NAMES = {
    'a' => 'a__',
    'a'*257 => 'a'*254+'__',
    'abcD' => 'abcd',
    'a-b*' => 'a_b_',
  }

  describe 'normalizer' do
    it 'normalized_msgpack should convert Bignum into String' do
      h = {'key' => 1111111111111111111111111111111111}
      unpacked = MessagePack.unpack(API.normalized_msgpack(h))
      expect(unpacked['key']).to eq(h['key'].to_s)
    end

    it 'normalized_msgpack with out argument should convert Bignum into String' do
      h = {'key' => 1111111111111111111111111111111111}
      out = ''
      API.normalized_msgpack(h, out)
      unpacked = MessagePack.unpack(out)
      expect(unpacked['key']).to eq(h['key'].to_s)
    end

    it 'normalize_database_name should return normalized data' do
      INVALID_NAMES.each_pair {|ng,ok|
        API.normalize_database_name(ng).should == ok
      }
      lambda {
        API.normalize_database_name('')
      }.should raise_error(RuntimeError)
    end

    it 'normalize_table_name should return normalized data' do
      INVALID_NAMES.each_pair {|ng,ok|
        API.normalize_table_name(ng).should == ok
      }
      lambda {
        API.normalize_table_name('')
      }.should raise_error(RuntimeError)
    end

    it 'normalize_database_name should return valid data' do
      VALID_NAMES.each {|ok|
        API.normalize_database_name(ok).should == ok
      }
    end
  end

  describe 'validator' do
    it 'validate_database_name should raise errors' do
      INVALID_NAMES.each_pair {|ng,ok|
        lambda {
          API.validate_database_name(ng)
        }.should raise_error(RuntimeError)
      }
      lambda {
        API.validate_database_name('')
      }.should raise_error(RuntimeError)
    end

    it 'validate_table_name should raise errors' do
      INVALID_NAMES.each_pair {|ng,ok|
        lambda {
          API.validate_table_name(ng)
        }.should raise_error(RuntimeError)
      }
      lambda {
        API.validate_table_name('')
      }.should raise_error(RuntimeError)
    end

    it 'validate_database_name should return valid data' do
      VALID_NAMES.each {|ok|
        API.validate_database_name(ok)
      }
    end
  end
end
