require 'test_store'
require 'koya/quoya'

class VillaStoreTest < Test::Unit::TestCase
  include KoyaStoreTestMixin

  def setup
    @koya = Koya::VillaStore.new(@dbname)
  end

  def teardown
    File::unlink(@dbname)
  end
end
