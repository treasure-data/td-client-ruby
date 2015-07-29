require 'spec_helper'

describe API do
  describe '#completed_body?' do
    let(:api) { TreasureData::API.new('')  }
    let(:response) { double(:response) }

    subject { api.__send__(:completed_body?, response) }

    context 'response has no content length' do
      before do
        response.stub_chain(:header, :content_length).and_return(nil)
      end

      it { is_expected.to be }
    end

    context 'response has content length' do
      let(:content_length) { 10 }

      before do
        response.stub_chain(:header, :content_length).and_return(content_length)
      end

      context 'content length equal body size' do
        before do
          response.stub(:body).and_return('a' * content_length)
        end

        it { is_expected.to be }
      end

      context 'content length lager than body size' do
        before do
          response.stub(:body).and_return('a' * (content_length - 1))
        end

        it { is_expected.not_to be }
      end

      context 'content length less than body size' do
        before do
          response.stub(:body).and_return('a' * (content_length + 1))
        end

        it { is_expected.not_to be }
      end
    end
  end
end
