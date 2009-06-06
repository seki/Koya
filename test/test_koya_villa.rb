require 'test/unit'
require 'koya_test_mixin'
require 'koya/quoya'

if __FILE__ == $0
  class VillaKoyaTest < Test::Unit::TestCase
    include KoyaTestMixin

    def setup
      @koya = Koya::VillaStore.new(@dbname)
    end

    def teardown
      File::unlink(@dbname)
    end
  end
end
