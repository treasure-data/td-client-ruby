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
    'a'*257 => 'a'*253+'__',
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
      # empty
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
    describe "'validate_database_name'" do
      it 'should raise a ParameterValidationError exceptions' do
        INVALID_NAMES.each_pair {|ng,ok|
          lambda {
            API.validate_database_name(ng)
          }.should raise_error(ParameterValidationError)
        }
        # empty
        lambda {
          API.validate_database_name('')
        }.should raise_error(ParameterValidationError)
      end

      it 'should return valid data' do
        VALID_NAMES.each {|ok|
          API.validate_database_name(ok).should == ok
        }
      end
    end

    describe "'validate_table_name'" do
      it 'should raise a ParameterValidationError exception' do
        INVALID_NAMES.each_pair {|ng,ok|
          lambda {
            API.validate_table_name(ng)
          }.should raise_error(ParameterValidationError)
        }
        lambda {
          API.validate_table_name('')
        }.should raise_error(ParameterValidationError)
      end

      it 'should return valid data' do
        VALID_NAMES.each {|ok|
          API.validate_database_name(ok).should == ok
        }
      end
    end

    describe "'validate_result_set_name'" do
      it 'should raise a ParameterValidationError exception' do
        INVALID_NAMES.each_pair {|ng,ok|
          lambda {
            API.validate_result_set_name(ng)
          }.should raise_error(ParameterValidationError)
        }
        # empty
        lambda {
          API.validate_result_set_name('')
        }.should raise_error(ParameterValidationError)
      end

      it 'should return valid data' do
        VALID_NAMES.each {|ok|
          API.validate_result_set_name(ok).should == ok
        }
      end
    end

    describe "'validate_column_name'" do
      it 'should raise a ParameterValidationError exception' do
        ['/', '', 'D'].each { |ng|
          lambda {
            API.validate_column_name(ng)
          }.should raise_error(ParameterValidationError)
        }
      end

      it 'should return valid data' do
        VALID_NAMES.each {|ok|
          API.validate_column_name(ok).should == ok
        }
        # columns can be as short as 2 characters
        API.validate_column_name('ab').should == 'ab'
      end
    end


    describe "'generic validate_name'" do
      it 'should raise a ParameterValidationError exception' do
        INVALID_NAMES.each_pair {|ng,ok|
          lambda {
            API.validate_name("generic", 3, 256, ng)
          }.should raise_error(ParameterValidationError)
        }
        # empty
        lambda {
          API.validate_name("generic", 3, 256, '')
        }.should raise_error(ParameterValidationError)
        # too short - one less than left limit
        lambda {
          API.validate_name("generic", 3, 256, 'ab')
        }.should raise_error(ParameterValidationError)
      end

      it 'should return valid data' do
        VALID_NAMES.each {|ok|
          API.validate_name("generic", 3, 256, ok).should == ok
        }
        # esplore left boundary
        API.validate_name("generic", 2, 256, 'ab').should == 'ab'
        API.validate_name("generic", 1, 256, 'a').should == 'a'
        # explore right boundary
        API.validate_name("generic", 3, 256, 'a' * 256).should == 'a' * 256
        API.validate_name("generic", 3, 128, 'a' * 128).should == 'a' * 128
      end
    end

    describe 'checking GET API content length with ssl' do
      include_context 'common helper'

      let(:api) { API.new(nil, endpoint: endpoint) }
      let :packed do
        s = StringIO.new
        Zlib::GzipWriter.wrap(s) do |f|
          f << ['hello', 'world'].to_json
        end
        s.string
      end

      before do
        stub_api_request(:get, '/v3/job/result/12345', ssl: ssl).
          with(:query => {'format' => 'json'}).
          to_return(
            :headers => {'Content-Encoding' => 'gzip'}.merge(content_length),
            :body => packed
          )
      end

      subject (:get_api_call) {
        api.job_result_format(12345, 'json', StringIO.new)
      }

      context 'without ssl' do
        let(:endpoint) { "http://#{API::DEFAULT_ENDPOINT}" }
        let(:ssl) { false }
        let(:content_length) { {'Content-Length' => packed.size} }

        it 'not called #completed_body?' do
          api.should_not_receive(:completed_body?)

          get_api_call
        end
      end

      context 'with ssl' do
        let(:endpoint) { "https://#{API::DEFAULT_ENDPOINT}" }
        let(:ssl) { true }

        context 'without Content-Length' do
          let(:content_length) { {} }

          it 'api accuess succeded' do
            expect { get_api_call }.not_to raise_error
          end
        end

        context 'with Content-Length' do
          context 'macth Content-Length and body.size' do
            let(:content_length) { {'Content-Length' => packed.size} }

            it 'api accuess succeded' do
              expect { get_api_call }.not_to raise_error
            end
          end

          context 'not macth Content-Length and body.size' do
            let(:content_length) { {'Content-Length' => packed.size + 1} }

            it 'api accuess succeded' do
              expect { get_api_call }.to raise_error(TreasureData::API::IncompleteError)
            end
          end
        end
      end
    end
  end
end
