require 'spec_helper'

describe APIError do
  let (:message){ 'message' }
  let (:api_backtrace){ double('api_backtrace') }
  describe 'new' do
    context '' do
      it do
        exc = APIError.new(message, api_backtrace)
        expect(exc).to be_an(APIError)
        expect(exc.message).to eq message
        expect(exc.api_backtrace).to eq api_backtrace
      end
    end
    context 'api_backtrace is ""' do
      let (:api_backtrace){ '' }
      it do
        exc = APIError.new(message, api_backtrace)
        expect(exc).to be_an(APIError)
        expect(exc.message).to eq message
        expect(exc.api_backtrace).to be_nil
      end
    end
    context 'api_backtrace is nil' do
      let (:api_backtrace){ nil }
      it do
        exc = APIError.new(message, api_backtrace)
        expect(exc).to be_an(APIError)
        expect(exc.message).to eq message
        expect(exc.api_backtrace).to be_nil
      end
    end
  end
end

describe AlreadyExistsError do
  let (:message){ 'message' }
  let (:api_backtrace){ double('api_backtrace') }
  let (:conflicts_with){ '12345' }
  describe 'new' do
    context '' do
      it do
        exc = AlreadyExistsError.new(message, api_backtrace)
        expect(exc).to be_an(AlreadyExistsError)
        expect(exc.message).to eq message
        expect(exc.api_backtrace).to eq api_backtrace
      end
    end
    context 'api_backtrace is ""' do
      let (:api_backtrace){ '' }
      it do
        exc = AlreadyExistsError.new(message, api_backtrace)
        expect(exc).to be_an(AlreadyExistsError)
        expect(exc.message).to eq message
        expect(exc.api_backtrace).to be_nil
      end
    end
    context 'api_backtrace is nil' do
      let (:api_backtrace){ nil }
      it do
        exc = AlreadyExistsError.new(message, api_backtrace)
        expect(exc).to be_an(AlreadyExistsError)
        expect(exc.message).to eq message
        expect(exc.api_backtrace).to be_nil
      end
    end
    context 'conflict' do
      it do
        exc = AlreadyExistsError.new(message, api_backtrace, conflicts_with)
        expect(exc).to be_an(AlreadyExistsError)
        expect(exc.message).to eq message
        expect(exc.api_backtrace).to eq api_backtrace
        expect(exc.conflicts_with).to eq conflicts_with
      end
    end
  end
end
