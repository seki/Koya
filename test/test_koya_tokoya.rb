require 'koya'
require 'test/unit'
require 'koya_test_mixin'
require 'koya/tokoya'

if __FILE__ == $0
  class TokoyaKoyaTest < Test::Unit::TestCase
    include KoyaTestMixin

    def setup
      @koya = Koya::TokoyaStore.new(@dbname)
    end

    def teardown
      File::unlink(@dbname)
    end
  end
end
