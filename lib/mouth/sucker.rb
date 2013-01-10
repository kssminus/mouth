require 'em-mongo'
require 'eventmachine'

module Mouth
  
  class SuckerConnection < EM::Connection
    attr_accessor :sucker
  
    def receive_data(data)
      Mouth.logger.debug "UDP packet: '#{data}'"
  
      sucker.store!(data)
    end
  end
  
  class Sucker
    
    # Host/Port to suck UDP packets on
    attr_accessor :host
    attr_accessor :port

    # Actual EM::Mongo connection
    attr_accessor :mongo
    
    # Info to connect to mongo
    attr_accessor :mongo_db_name
    attr_accessor :mongo_hostports
    
    attr_accessor :steps
    attr_accessor :stepping
    
    # Stats
    attr_accessor :udp_packets_received
    attr_accessor :mongo_flushes
    
    def initialize(options = {})
      self.host = options[:host] || "localhost"
      self.port = options[:port] || 8889
      self.mongo_db_name = options[:mongo_db_name] || "mouth"
      hostports = options[:mongo_hostports] || [["localhost", EM::Mongo::DEFAULT_PORT]]
      self.mongo_hostports = hostports.collect do |hp|
        if hp.is_a?(String)
          host, port = hp.split(":")
          [host, port || EM::Mongo::DEFAULT_PORT]
        else
          hp
        end
      end
      
      self.udp_packets_received = 0
      self.mongo_flushes = 0
      
      self.steps = {}
      self.stepping = {}
    end
    
    def suck!
      EM.run do
        # Connect to mongo now
        self.mongo

        EM.open_datagram_socket host, port, SuckerConnection do |conn|
          conn.sucker = self
        end
        
        EM.add_periodic_timer(5) do
          Mouth.logger.info "Steps: #{self.steps.inspect}"
          Mouth.logger.info "Stepping: #{self.stepping.inspect}"
          self.flush!
          self.set_procline!
        end

        EM.next_tick do
          Mouth.logger.info "Mouth reactor started..."
          self.set_procline!
        end
      end
    end
    
    # steps: safaritour.uuid:4|s
    # stepping: safaritour.uuid:4|su
    def store!(data)
      key_value, command = data.to_s.split("|", 2)
      key, value = key_value.to_s.split(":")
      
      return unless key && value && command && key.length > 0 && value.length > 0 && command.length > 0
      
      key = Mouth.parse_key(key).join(":")
      value = value.to_i
      
      #ts = Mouth.current_timestamp
      ts  = Time.now.to_i

      if command == "s"
        self.steps[key] ||= {}
        self.steps[key]["t"]  = ts
        self.steps[key]["ms"] = value #MAX STEP
        self.steps[key]["cs"] = 0     #CURRENT STEP
      elsif command == "su"
        self.stepping[key] ||= {}
        self.stepping[key]["t"]   = ts
        self.stepping[key]["cs"]  ||= 0
        self.stepping[key]["cs"]  += value
      end
      
      self.udp_packets_received += 1
    end
    
    def flush!
      ts = Time.now.to_i
      limit_ts = ts - 30
      mongo_docs = []
      
      # We're going to construct mongo_docs which look like this:
      # "mycollection:stepid": {
      #   t:  23423433,
      #   ms: 6,
      #   cs: 1
      # }
      
      self.steps.each do |step_id, step_to_save|
        if step_to_save["t"] <= limit_ts
          
          mongo_docs.push({step_id => step_to_save})
          self.steps.delete(step_id)
        end
      end
      
      self.stepping.each do |stepping_id, stepping_to_save|
        if stepping_to_save["t"] <= limit_ts
          self.stepping.delete(stepping_id)
          #mongo_docs.push({stepping_id => {"t"=>ts, "cs"=>0}})
          mongo_docs.push({stepping_id => {"$inc"=>{"cs"=>stepping_to_save["cs"]}}})
        end
      end
      #"mycollection":{ "$inc":{ "cs": 3} }
      
      save_documents!(mongo_docs)
    end
    
    def save_documents!(mongo_docs)
      
      mongo_docs.each do |doc|
        doc.each do |key, step|
          ns, step_id = key.split(":")
          collection_name = Mouth.mongo_collection_name(ns)
          #step["si"] = step_id
          
          self.mongo.collection(collection_name).update({"si"=>step_id} , step, {:upsert => false})
          Mouth.logger.info "Saving Doc(si:#{step_id}): #{step.inspect}"
        end
      end
      
      
      self.mongo_flushes += 1 if mongo_docs.any?
    end
    
    def mongo
      @mongo ||= begin
        if self.mongo_hostports.length == 1
          EM::Mongo::Connection.new(*self.mongo_hostports.first).db(self.mongo_db_name)
        else
          raise "Ability to connect to a replica set not implemented."
        end
      end
    end
    
    def set_procline!
      $0 = "mouth [started] [UDP Recv: #{self.udp_packets_received}] [Mongo saves: #{self.mongo_flushes}]"
    end
    
  end # class Sucker
end # module
