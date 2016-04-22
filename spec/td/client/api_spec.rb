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
        expect(API.normalize_database_name(ng)).to eq(ok)
      }
      expect {
        API.normalize_database_name('')
      }.to raise_error(RuntimeError)
    end

    it 'normalize_table_name should return normalized data' do
      INVALID_NAMES.each_pair {|ng,ok|
        expect(API.normalize_table_name(ng)).to eq(ok)
      }
      # empty
      expect {
        API.normalize_table_name('')
      }.to raise_error(RuntimeError)
    end

    it 'normalize_database_name should return valid data' do
      VALID_NAMES.each {|ok|
        expect(API.normalize_database_name(ok)).to eq(ok)
      }
    end
  end

  describe 'validator' do
    describe "'validate_database_name'" do
      it 'should raise a ParameterValidationError exceptions' do
        INVALID_NAMES.each_pair {|ng,ok|
          expect {
            API.validate_database_name(ng)
          }.to raise_error(ParameterValidationError)
        }
        # empty
        expect {
          API.validate_database_name('')
        }.to raise_error(ParameterValidationError)
      end

      it 'should return valid data' do
        VALID_NAMES.each {|ok|
          expect(API.validate_database_name(ok)).to eq(ok)
        }
      end
    end

    describe "'validate_table_name'" do
      it 'should raise a ParameterValidationError exception' do
        INVALID_NAMES.each_pair {|ng,ok|
          expect {
            API.validate_table_name(ng)
          }.to raise_error(ParameterValidationError)
        }
        expect {
          API.validate_table_name('')
        }.to raise_error(ParameterValidationError)
      end

      it 'should return valid data' do
        VALID_NAMES.each {|ok|
          expect(API.validate_database_name(ok)).to eq(ok)
        }
      end
    end

    describe "'validate_result_set_name'" do
      it 'should raise a ParameterValidationError exception' do
        INVALID_NAMES.each_pair {|ng,ok|
          expect {
            API.validate_result_set_name(ng)
          }.to raise_error(ParameterValidationError)
        }
        # empty
        expect {
          API.validate_result_set_name('')
        }.to raise_error(ParameterValidationError)
      end

      it 'should return valid data' do
        VALID_NAMES.each {|ok|
          expect(API.validate_result_set_name(ok)).to eq(ok)
        }
      end
    end

    describe "'validate_column_name'" do
      it 'should raise a ParameterValidationError exception' do
        [''].each { |ng|
          expect {
            API.validate_column_name(ng)
          }.to raise_error(ParameterValidationError)
        }
      end

      it 'should return valid data' do
        VALID_NAMES.each {|ok|
          expect(API.validate_column_name(ok)).to eq(ok)
        }
        # columns can be as short as 2 characters
        expect(API.validate_column_name('ab')).to eq('ab')
      end
    end


    describe "'generic validate_name'" do
      it 'should raise a ParameterValidationError exception' do
        INVALID_NAMES.each_pair {|ng,ok|
          expect {
            API.validate_name("generic", 3, 256, ng)
          }.to raise_error(ParameterValidationError)
        }
        # empty
        expect {
          API.validate_name("generic", 3, 256, '')
        }.to raise_error(ParameterValidationError)
        # too short - one less than left limit
        expect {
          API.validate_name("generic", 3, 256, 'ab')
        }.to raise_error(ParameterValidationError)
      end

      it 'should return valid data' do
        VALID_NAMES.each {|ok|
          expect(API.validate_name("generic", 3, 256, ok)).to eq(ok)
        }
        # esplore left boundary
        expect(API.validate_name("generic", 2, 256, 'ab')).to eq('ab')
        expect(API.validate_name("generic", 1, 256, 'a')).to eq('a')
        # explore right boundary
        expect(API.validate_name("generic", 3, 256, 'a' * 256)).to eq('a' * 256)
        expect(API.validate_name("generic", 3, 128, 'a' * 128)).to eq('a' * 128)
      end
    end

    describe 'checking GET API content length with ssl' do
      include_context 'common helper'

      let(:api) { API.new(nil, endpoint: endpoint) }
      let :packed do
        s = StringIO.new(String.new)
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
        api.job_result_format(12345, 'json', StringIO.new(String.new))
      }

      context 'without ssl' do
        let(:endpoint) { "http://#{API::DEFAULT_ENDPOINT}" }
        let(:ssl) { false }
        let(:content_length) { {'Content-Length' => packed.size} }

        it 'not called #completed_body?' do
          expect(api).not_to receive(:completed_body?)

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
          context 'match Content-Length and body.size' do
            let(:content_length) { {'Content-Length' => packed.size} }

            it 'api accuess succeded' do
              expect { get_api_call }.not_to raise_error
            end
          end
        end
      end
    end
  end
end
