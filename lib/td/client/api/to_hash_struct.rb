class TreasureData::API
  class ToHashStruct < Struct
    module ClassModule
      def parse_json(body)
        begin
          js = JSON.load(body)
        rescue
          raise "Unexpected API response: #{$!}"
        end
        unless js.is_a?(Hash)
          raise "Unexpected API response: #{body}"
        end
        js
      end

      def from_json(json)
        from_hash(parse_json(json))
      end

      def from_hash(hash)
        return new if hash.nil?
        new(*members.map { |sym|
          v = hash[sym] || hash[sym.to_s]
          model.key?(sym) ? model[sym].from_hash(v) : v
        })
      end

      def model_property(key, klass)
        model[key.to_sym] = klass
      end

      def model
        @model ||= {}
      end
    end
    extend ClassModule

    def to_h
      self.class.members.inject({}) { |r, e|
        v = self[e]
        r[e.to_s] = v.respond_to?(:to_h) ? v.to_h : v
        r
      }
    end

    def to_json
      to_h.to_json
    end

    def validate
      validate_self
      values.each do |v|
        v.validate if v.is_a?(ToHashStruct)
      end
      self
    end

    def validate_self
      # define as required
    end

  private

    def validate_presence_of(key)
      unless self.send(key)
        raise ArgumentError.new("#{key} required")
      end
    end
  end
end
