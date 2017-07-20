require 'spec_helper'
require 'td/client/spec_resources'

describe 'TreasureData::Schema::Field' do
  describe '.new' do
    context 'name="v"' do
      it 'raises ParameterValidationError' do
        expect{ Schema::Field.new('v', 'int') }.to raise_error(ParameterValidationError)
      end
    end
    context 'name="time"' do
      it 'raises ParameterValidationError' do
        expect{ Schema::Field.new('time', 'int') }.to raise_error(ParameterValidationError)
      end
    end
    context 'name with UTF-8' do
      it 'works' do
        name = "\u3042\u3044\u3046"
        f = Schema::Field.new(name, 'int')
        expect(f.name).to eq name
        expect(f.type).to eq 'int'
        expect(f.sql_alias).to be_nil
      end
    end
    context 'with sql_alias' do
      it 'raises' do
        f = Schema::Field.new('t:t', 'int', 'alice')
        expect(f.name).to eq 't:t'
        expect(f.type).to eq 'int'
        expect(f.sql_alias).to eq 'alice'
      end
    end
    context 'with sql_alias which equals to its name' do
      it 'works' do
        name = "abc"
        f = Schema::Field.new(name, 'int', name)
        expect(f.name).to eq name
        expect(f.type).to eq 'int'
        expect(f.sql_alias).to eq name
      end
    end
    context 'with invalid sql_alias' do
      it 'raises' do
        expect{ Schema::Field.new('t:t', 'int', 't:t') }.to raise_error(ParameterValidationError)
      end
    end
  end
end

describe 'TreasureData::Schema' do
  describe '.parse' do
    let(:columns){ ["foo:int", "BAR\u3070\u30FC:string@bar", "baz:baz!:array<double>@baz"] }
    it do
      sc = Schema.parse(columns)
      expect(sc.fields.size).to eq 3
      expect(sc.fields[0].name).to eq 'foo'
      expect(sc.fields[0].type).to eq 'int'
      expect(sc.fields[0].sql_alias).to be_nil
      expect(sc.fields[1].name).to eq "BAR\u3070\u30FC"
      expect(sc.fields[1].type).to eq 'string'
      expect(sc.fields[1].sql_alias).to eq 'bar'
      expect(sc.fields[2].name).to eq 'baz:baz!'
      expect(sc.fields[2].type).to eq 'array<double>'
      expect(sc.fields[2].sql_alias).to eq 'baz'
    end
  end

  describe '.new' do
    it 'works with single field' do
      f = Schema::Field.new('a', 'int')
      sc = Schema.new([f])
      expect(sc.fields[0]).to eq f
    end
    it 'works with multiple fields' do
      f0 = Schema::Field.new('a', 'int')
      f1 = Schema::Field.new('b', 'int', 'b')
      sc = Schema.new([f0, f1])
      expect(sc.fields[0]).to eq f0
      expect(sc.fields[1]).to eq f1
    end
    it 'raises' do
      f0 = Schema::Field.new('a', 'int')
      f1 = Schema::Field.new('b', 'int', 'a')
      expect{ Schema.new([f0, f1]) }.to raise_error(ArgumentError)
    end
  end

  describe '#fields' do
    it do
      f = Schema::Field.new('a', 'int')
      sc = Schema.new([f])
      expect(sc.fields[0]).to eq f
    end
  end

  describe '#add_field' do
    it do
      f = Schema::Field.new('a', 'int')
      sc = Schema.new([f])
      sc.add_field('b', 'double', 'bb')
      expect(sc.fields[1].name).to eq 'b'
    end
    it do
      f = Schema::Field.new('a', 'int')
      sc = Schema.new([f])
      sc.add_field('b', 'double', 'b')
      expect(sc.fields[1].name).to eq 'b'
    end
    it 'raises ParameterValidationError if name is duplicated' do
      f = Schema::Field.new('a', 'int')
      sc = Schema.new([f])
      expect{ sc.add_field('a', 'double') }.to raise_error(ParameterValidationError)
    end
    it 'raises ParameterValidationError if sql_alias is duplicated' do
      f = Schema::Field.new('a', 'int')
      sc = Schema.new([f])
      expect{ sc.add_field('abc', 'double', 'a') }.to raise_error(ParameterValidationError)
    end
  end

  describe '#merge' do
    it do
      sc1 = Schema.parse(['foo:int', 'bar:float'])
      sc2 = Schema.parse(['bar:double', 'baz:string'])
      sc3 = sc1.merge(sc2)
      expect(sc3.fields.size).to eq 3
      expect(sc3.fields[0].name).to eq 'foo'
      expect(sc3.fields[0].type).to eq 'int'
      expect(sc3.fields[1].name).to eq 'bar'
      expect(sc3.fields[1].type).to eq 'double'
      expect(sc3.fields[2].name).to eq 'baz'
      expect(sc3.fields[2].type).to eq 'string'
    end
    it do
      sc1 = Schema.parse(['foo:int', 'bar:float'])
      sc2 = Schema.parse(['bar:double@foo'])
      expect{ sc1.merge(sc2) }.to raise_error(ArgumentError)
    end
  end

  describe '#to_json' do
    it do
      sc = Schema.parse(['foo:int', 'bar:float@baz'])
      expect(sc.to_json).to eq '[["foo","int"],["bar","float","baz"]]'
    end
  end

  describe '#from_json' do
    it do
      sc = Schema.new
      sc.from_json [["foo","int"],["bar","float","baz"]]
      expect(sc.fields.size).to eq 2
      expect(sc.fields[0].name).to eq 'foo'
      expect(sc.fields[0].type).to eq 'int'
      expect(sc.fields[0].sql_alias).to be_nil
      expect(sc.fields[1].name).to eq 'bar'
      expect(sc.fields[1].type).to eq 'float'
      expect(sc.fields[1].sql_alias).to eq 'baz'
    end
  end
end
