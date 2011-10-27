module Mouth
  
  class Record
    
    # Keys are symbols
    # id is :id, not _id
    attr_accessor :attributes
    
    def initialize(attrs = {})
      self.attributes = normalize_attributes(attrs)
    end
    
    def save
      if self.attributes[:id]
        attrs = self.attributes.dup
        the_id = attrs.delete(:id).to_s
        doc = self.class.collection.update({"_id" => BSON::ObjectId(the_id)}, attrs)
      else
        self.class.collection.insert(self.attributes)
        self.attributes[:id] = self.attributes.delete(:_id).to_s
      end
      true
    end
    
    def normalize_attributes(attrs)
      normalize = lambda do |h|
        hd = {}
        h.each_pair do |key, val|
          val = normalize.call(val) if val.is_a?(Hash)
          val = val.to_s if val.is_a?(BSON::ObjectId)
          # TODO: arrays :(
          hd[key.to_s == "_id" ? :id : key.to_sym] = val
        end
        hd
      end
      normalize.call attrs
    end
    
    def self.collection
      demodularized = self.to_s.match(/(.+::)?(.+)$/)[2] || "record"
      tableized = demodularized.downcase + "s" # (: lol :)
      @collection ||= Mouth.mongo.collection(tableized)
    end
    
    def self.create(attributes)
      r = new(attributes)
      r.save
      r
    end
    
    def self.all
      collection.find.to_a.collect {|d| new(d) }
    end
  end
end
