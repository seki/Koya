require 'koya'

class Koya
  class LogStream < KoyaStream
    def initialize
      super
      self.limit = nil
    end
    koya_attr(:limit)

    def push(value)
      transaction do
        super(value)
        limit = self.limit
        while (limit && self.size > limit)
          self.shift
        end
      end
    end

    undef :unshift
    undef :pop

    private
    def store(index, value)
      @_koya_[index] = value
      @_koya_["#{index}.time"] = Time.now
    end
    
    def fetch(index, remove=false)
      [@_koya_["#{index}.time"], @_koya_[index]]
    ensure
      @_koya_.delete(index) if remove
      @_koya_.delete("#{index}.time") if remove
    end
  end
end

if __FILE__ == $0
  db = Koya::Store.new('log.db')
  db.transaction do
    unless db.root['log'] 
      db.root['log'] = Koya::LogStream.new
      db.root['log'].limit = 5
    end
  end
  
  log = db.root['log']
  log << ARGV
  log.to_a.each do |v|
    p v
  end
end
