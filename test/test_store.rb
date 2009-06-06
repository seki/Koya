require 'test/unit'
require 'koya/koya'

module KoyaStoreTestMixin
  def initialize(*arg)
    super(*arg)
    @dbname = 'koya_test.db'
  end

  def setup
    @koya = Koya::Store.new(@dbname)
  end

  def teardown
    File::unlink(@dbname)
  end

  def test_00_stuff
    ref = @koya.create_object(Koya::KoyaObject)
    assert_equal(@koya, ref._koya_store_)
    row = ref._koya_rowid_
    assert_equal('2', row)
    
    stuff = Koya::KoyaStuff.new(ref)
    assert_equal(nil, stuff['@foo'])
    stuff['@foo'] = 'foo'
    assert_equal('foo', stuff['@foo'])

    stuff['@foo'] = 1
    assert_equal(1, stuff['@foo'])

    stuff['@foo'] = 1.0
    assert_equal(1.0, stuff['@foo'])

    stuff['@foo'] = ref
    assert_equal(ref, stuff['@foo'])

    assert_equal(['@foo'], stuff.keys)

    stuff.delete('@foo')
    assert_equal([], stuff.keys)

    assert_equal([], stuff.values)

    stuff['@bar'] = 'bar'
    assert_equal(['bar'], stuff.values)
    stuff['@baz'] = 'baz'
    assert_equal(['bar', 'baz'], stuff.values.sort)
    
    ary = []
    
    stuff.each {|k, v| ary.push([k, v])}
    assert_equal([['@bar', 'bar'], ['@baz', 'baz']], ary.sort)

    ary = stuff.to_a
    assert_equal([['@bar', 'bar'], ['@baz', 'baz']], ary.sort)
    assert_equal(['@bar', '@baz'], stuff.keys)
    
    assert_equal(7, @koya.revisions.size)

    assert_equal(@koya.revision.to_f, @koya.revisions[0].to_f)
  end

  def test_02_gc
    ref = @koya.create_object(Koya::KoyaObject)
    stuff = Koya::KoyaStuff.new(ref)
    stuff.lock
    assert_equal([], @koya.referer(stuff.rowid))
    @koya.root['foo'] = ref
    assert_equal(1, @koya.referer(stuff.rowid).size)
    @koya.root['foo'] = nil

    @koya.gc
    assert_equal("Koya::KoyaObject",  @koya.get_klass(ref._koya_rowid_))
    stuff.unlock
    @koya.gc
    assert_equal(nil,  @koya.get_klass(ref._koya_rowid_))

    @koya.root['foo'] = @koya.create_object(Koya::KoyaObject)
    @koya.root['bar'] = @koya.create_object(Koya::KoyaObject)
    @koya.root['baz'] = @koya.create_object(Koya::KoyaObject)
    
    assert_equal(3, @koya.extent(Koya::KoyaObject).size)
  end

  def test_01_store
    rowid = @koya.create_object(Koya::KoyaObject)._koya_rowid_
    assert_equal(true, @koya.lock_object(rowid))
    assert_equal(false, @koya.lock_object(rowid))
    @koya.unlock_object(rowid)
    assert_equal(true, @koya.lock_object(rowid))
    assert_equal(false, @koya.lock_object(rowid))
    @koya.unlock_object(rowid)
    assert_equal('Koya::KoyaObject', @koya.get_klass(rowid))
    
    assert_equal('foo', @koya.set_prop(rowid, '@foo', 'foo'))
    @koya.set_prop(rowid, '@bar', 'bar')
    @koya.set_prop(rowid, '@baz', 'baz')
    @koya.set_prop(rowid, '0', 0)

    assert_equal([['0', 0], ['@bar', 'bar'], ['@baz', 'baz'], ['@foo', 'foo']],
                 @koya.all_prop(rowid).sort)

    assert_equal([['@bar', 'bar'], ['@baz', 'baz'], ['@foo', 'foo']],
                 @koya.all_ivars(rowid).sort)
  end
end

if __FILE__ == $0
  require 'koya/sqlite_store'

  class KoyaStoreTest < Test::Unit::TestCase
    include KoyaStoreTestMixin
    def setup
      @koya = Koya::SQLiteStore.new(@dbname)
    end

    def teardown
      File::unlink(@dbname)
    end
  end
end
