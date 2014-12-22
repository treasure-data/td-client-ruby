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
        if @model
          hash = hash.inject({}) { |r, (k, v)|
            r[k] = @model.key?(k.to_sym) ? @model[k.to_sym].from_hash(v) : v
            r
          }
        end
        new(*hash.values_at(*members.map(&:to_s)))
      end

      def model_property(key, klass)
        (@model ||= {})[key.to_sym] = klass
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
        v.validate if v.respond_to?(:validate)
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
