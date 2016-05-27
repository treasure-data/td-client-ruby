require 'spec_helper'
require 'td/client/spec_resources'

describe 'Schedule Model' do
  describe '#run' do
    let(:api_key) { '1234567890abcd' }
    let(:api) { double(:api) }
    let(:client) { Client.new(api_key) }
    let(:name) { 'schedule' }
    let(:schedule) {
      Schedule.new(client, name, '0 0 * * * *', 'select 1')
    }
    let(:time) { "2013-01-01 00:00:00"  }
    let(:num) { 1 }

    before do
      allow(API).to receive(:new).with(api_key, {}).and_return(api)
    end

    it 'success call api' do
      expect(api).to receive(:run_schedule).with(name, time, num).and_return([])

      schedule.run(time, num)
    end
  end
end
