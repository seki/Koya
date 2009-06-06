require 'koya/koya'
require 'koya/klass'
require 'koya/tokoya'

class Koya
  Store = TokoyaStore

  def self.new(name='koya.db')
    Store.new(name)
  end
end
