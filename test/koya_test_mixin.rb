require 'test/unit'

module KoyaTestMixin
  def initialize(*arg)
    super(*arg)
    @dbname = 'koya_test.db'
  end

  def test_create_twice
    assert_equal('0.5', @koya.version)

    first = @koya
    second = @koya.class.new(@dbname)

    assert_equal('0.5', second.version)

    assert_equal(nil, first['foo'])
    first['foo'] = 123.0

    assert_equal(123.0, first['foo'])
    assert_equal(123.0, second['foo'])
  end

  def test_marshal_with_zero
    @koya['array'] = [0, 1, 2, 3]

    assert_equal([0, 1, 2, 3], @koya['array'])
  end

  class MyNode < Koya::KoyaObject
    attr_accessor(:left, :right, :value)

    def lock
      @_koya_.lock
    end

    def unlock
      @_koya_.unlock
    end
  end

  def test_gc
    root = @koya.root
    
    root.transaction do
      root['top'] = MyNode.new
      root['top'].left = MyNode.new
      root['top'].right = MyNode.new
    end

    top = root['top']

    right = top.right
    assert_equal(true, right.lock)
    right.value = 'memo'

    left = top.left

    left.transaction do
      left.value = Koya::Stream.new
      100.times do |n|
        left.value.push(n)
      end
    end

    assert_equal(50, left.value[50])
    root.delete('top')

    @koya.gc

    assert_raise(Koya::ObjectNotFound) do
      left.value
    end
    assert_equal('memo', right.value)

    right.unlock

    @koya.gc

    assert_raise(Koya::ObjectNotFound) do
      right.value
    end
  end
  
  class MyKoya < Koya::KoyaObject
    attr_accessor('name')
  end

  def test_object
    assert_raise(Koya::TransactionNotFound) do
      MyKoya.new
    end
    @koya.transaction do
      @koya.root['tmp'] = MyKoya.new
    end

    m = @koya.root['tmp']
    assert_equal(nil, m.name)
    m.name = 'foo'
    assert_equal('foo', m.name)
  end

  class MyCell < Koya::KoyaObject
    def initialize(value)
      @value = value
    end

    attr_accessor('left')
    attr_accessor('right')
    attr_accessor('value')
    def push(value)
      transaction do
        raise '4' if value == 4
        cell = self.class.new(value)
        raise '5' if value == 5
        cell.right = @right
        raise '6' if value == 6
        cell.left = self
        raise '7' if value == 7
        @right = cell
        raise '8' if value == 8
      end
    end
  end

  def test_rollback
    @koya.transaction do
      @koya.root['cell'] = MyCell.new(1)
    end
    cell = @koya.root['cell']
    cell.push(2)
    cell.push(3)

    assert_equal(1, cell.value)
    assert_equal(3, cell.right.value)
    assert_equal(2, cell.right.right.value)
    assert_equal(nil, cell.right.right.right)

    cell.push(4) rescue nil
    @koya.gc

    assert_equal(1, cell.value)
    assert_equal(3, cell.right.value)
    assert_equal(2, cell.right.right.value)
    assert_equal(nil, cell.right.right.right)

    cell.push(5) rescue nil
    @koya.gc

    assert_equal(1, cell.value)
    assert_equal(3, cell.right.value)
    assert_equal(2, cell.right.right.value)
    assert_equal(nil, cell.right.right.right)

    cell.push(6) rescue nil
    @koya.gc

    assert_equal(1, cell.value)
    assert_equal(3, cell.right.value)
    assert_equal(2, cell.right.right.value)
    assert_equal(nil, cell.right.right.right)

    cell.push(7) rescue nil
    @koya.gc

    assert_equal(1, cell.value)
    assert_equal(3, cell.right.value)
    assert_equal(2, cell.right.right.value)
    assert_equal(nil, cell.right.right.right)

    cell.push(8) rescue nil
    @koya.gc

    assert_equal(1, cell.value)
    assert_equal(3, cell.right.value)
    assert_equal(2, cell.right.right.value)
    assert_equal(nil, cell.right.right.right)

    cell.push(9)
    @koya.gc

    assert_equal(1, cell.value)
    assert_equal(9, cell.right.value)
    assert_equal(3, cell.right.right.value)
    assert_equal(2, cell.right.right.right.value)
    assert_equal(nil, cell.right.right.right.right)
  end

  def test_stream
    @koya.transaction do
      @koya.root['stream'] = Koya::Stream.new
    end
    s = @koya.root['stream']

    assert_equal(nil, s.pop)

    s.push(1)
    s.push(2)
    s.push(3)

    assert_equal(3, s[2])
    assert_equal(2, s[1])
    assert_equal(1, s[0])
    assert_equal(3, s[-1])
    assert_equal(2, s[-2])
    assert_equal(1, s[-3])
    assert_equal(nil, s[4])
    assert_equal(nil, s[-4])

    assert_equal([1, 2], s[0, 2])
    assert_equal([1, 2], s[0..1])
    assert_equal([1, 2], s[-3..-2])
    assert_equal([], s[2..0])
    assert_equal(nil, s[0, -1])

    assert_equal(3, s.pop)
    assert_equal(2, s.pop)
    assert_equal(1, s.pop)
    assert_equal(nil, s.pop)

    s.push(1)
    s.push(2)
    s.push(3)

    assert_equal([1, 2, 3], s.to_a)

    assert_equal(1, s.shift)
    assert_equal(2, s.shift)
    assert_equal(3, s.shift)
    assert_equal(nil, s.pop)

    s.unshift(1)
    s.unshift(2)
    s.unshift(3)

    assert_equal([3, 2, 1], s.to_a)

    assert_equal(1, s.pop)
    assert_equal(2, s.pop)
    assert_equal(3, s.pop)
    assert_equal(nil, s.pop)
  end

  def test_stream_replace
    @koya.transaction do
      @koya.root['stream'] = Koya::Stream.new
    end
    s = @koya.root['stream']

    s.unshift(1)
    s.unshift(2)
    s.unshift(3)

    assert_equal([3, 2, 1], s.to_a)
 
    x = s.replace([1, 2, 3, 4])
    # assert_equal(x, s) #FIXME
    assert_equal([1, 2, 3, 4], s.to_a)

    s.replace([2, 3, 4])
    assert_equal([2, 3, 4], s.to_a)
  end

  class Counter < Koya::KoyaObject
    def initialize
      @value = 0
    end
    attr_accessor(:value)
    
    def up
      transaction do
        @value += 1
      end
    end

    def down
      transaction do
        @value -= 1
      end
    end
  end

  class SetElem < Koya::KoyaObject
    def initialize(name, value)
      @name = name
      @value = value
      @hidden = 1
    end
    attr_accessor :name, :value, :hidden
  end
  
  def test_set
    @koya.transaction do
      @koya.root['set'] = Koya::Set.new
    end
    s = @koya.root['set']

    ary = []
    s.transaction do
      ary.push(Counter.new)
      ary.push(Counter.new)
      ary.push(Counter.new)
      ary[1].up
      ary[2].up
      ary[2].up
      s.add(ary[0])
      s.add(ary[1])
      s.add(ary[2])
      s.add(ary[0])
      s.add(ary[1])
      s.add(ary[2])
    end

    assert_equal(ary, s.to_a.sort_by { |x| x.value })

    s.delete(ary[0])
    ary.shift

    assert_equal(ary, s.to_a.sort_by { |x| x.value })

    s.transaction do
      s.push(SetElem.new('foo', 1))
      s.push(SetElem.new('foo', 2))
      s.push(SetElem.new('boo', 3))
      s.push(SetElem.new('baz', 2))
      s.push(SetElem.new('baz', 2))
      s.push(SetElem.new('baz', 2))
      s.push(SetElem.new('obj', s))
    end

=begin
    ary = s.search({:@name => 'foo'}).collect {|x| x.name}
    assert_equal(['foo', 'foo'], ary)
=end

    s2 = @koya.transaction { @koya.root['set2'] = Koya::Set.new }
    s2.transaction do
      s2.push(SetElem.new('foo', 1))
      s2.push(SetElem.new('foo', 2))
    end

=begin
    ary = s.search({:@name => 'foo', :@value => 2})
    assert_equal(1, ary.size)
    assert_equal('foo', ary[0].name)
    assert_equal(2, ary[0].value)

    ary = s.search({:@name => 'baz', :@value => 2, :@hidden => 1})
    assert_equal(3, ary.size)
    assert_equal('baz', ary[0].name)

    ary = s.search({:@value => s})
    assert_equal(1, ary.size)
    assert_equal('obj', ary[0].name)

    ary = @koya.search(nil, {:@name => 'foo', :@value => 2})
    assert_equal(2, ary.size)
=end
  end

  def test_one_db_two_threads
    @koya.transaction do
      @koya.root['count'] = Counter.new
    end
    c = @koya.root['count']
    
    t1 = Thread.new do
      30.times do 
        c.up
      end
    end

    t2 = Thread.new do
      30.times do 
        c.down
      end
    end

    t1.join
    t2.join
    
    assert_equal(0, c.value)
  end

  def test_extent
    bag = @koya.transaction do
      @koya.root['bag'] = Koya::Stream.new
    end

    bag.transaction do
      bag.push MyNode.new
      bag.push MyKoya.new
      bag.push MyCell.new(bag[1])
      bag.push Counter.new
      bag.push Counter.new
      bag.push Counter.new
      bag.push MyNode.new
      bag.push MyCell.new(bag[1])
      bag.push MyCell.new(bag[1])
    end

    assert_equal(9, bag.size)

    ary = @koya.extent(Counter)
    
    assert_equal(3, ary.size)
    assert_kind_of(Koya::KoyaRef, ary[0])

    ary = @koya.extent(MyKoya)

    assert_equal(1, ary.size)
    ary[0].name = 'foo'

    assert_equal('foo', bag[1].name)
    
    assert_equal(ary[0], bag[1])

    ary = @koya.referer(bag[1]._koya_.rowid)
    assert_equal(4, ary.size)

    bag[7].value = nil
    ary = @koya.referer(bag[1]._koya_.rowid)
    assert_equal(3, ary.size)

    bag.pop
    ary = @koya.referer(bag[1]._koya_.rowid)
    assert_equal(2, ary.size)

    assert_equal(2, @koya.extent(MyCell).size)

    bag.pop
    assert_equal(1, @koya.extent(MyCell).size)
  end

  def test_revert_to
    bag = @koya.transaction do
      @koya.root['bag'] = Koya::Stream.new
    end

    bag.transaction do
      bag.push MyNode.new
      bag.push MyKoya.new
      bag.push MyCell.new(bag[1])
      bag.push Counter.new
      bag.push Counter.new
      bag.push Counter.new
      bag.push MyNode.new
      bag.push MyCell.new(bag[1])
      bag.push MyCell.new(bag[1])
    end

    rev = Time.now

    assert_equal(9, bag.size)
    
    bag.transaction do
      bag.push MyNode.new
      bag.push MyKoya.new
      bag.push MyCell.new(bag[1])
      bag.push Counter.new
      bag.push Counter.new
      bag.push Counter.new
      bag.push MyNode.new
      bag.push MyCell.new(bag[1])
      bag.push MyCell.new(bag[1])
    end
    bag[7].value = nil

    assert_equal(nil, bag[7].value)
    assert_equal(18, bag.size)

    rev2 = Time.now
    @koya.revert_to(rev)

    assert_equal(9, bag.size)
    assert_equal(bag[1], bag[7].value)

    @koya.revert_to(rev2)

    assert_equal(nil, bag[7].value)
    assert_equal(18, bag.size)
  end

  class ListCell < Koya::Cell
    def initialize(name)
      @name = name
    end
    attr_accessor :name

    def to_s
      @name.to_s
    end
  end

  def test_linked_list_cell
    one = two = three = nil

    @koya.transaction do
      one = ListCell.new('1')
      assert_equal('1', one.to_s)
      two = one.insert_right(ListCell.new('2'))
      assert_equal('2', two.to_s)
      three = two.insert_right(ListCell.new('3'))
      assert_equal('3', three.to_s)
      @koya.root['list'] = one
    end

    assert_equal('1 2 3', one.collect {|x| x.to_s}.join(' '))

    one.transaction do
      three.detach
      ary = one.left_end.collect { |x| x.to_s }
      assert_equal('1 2', ary.join(' '))

      two.insert_right(three)
      ary = one.left_end.collect { |x| x.to_s }
      assert_equal('1 2 3', ary.join(' '))

      two.detach
      ary = one.collect { |x| x.to_s }
      assert_equal('1 3', ary.join(' '))
      
      one.insert_left(two)
      ary = one.left_end.collect { |x| x.to_s }
      assert_equal('2 1 3', ary.join(' '))

      two.move_right
      ary = one.left_end.collect { |x| x.to_s }
      assert_equal('1 2 3', ary.join(' '))
    end

    assert_equal(one.to_s, one.left_end.to_s)
    assert_equal(one.to_s, two.left_end.to_s)
    assert_equal(one.to_s, three.left_end.to_s)
    assert_equal(three.to_s, one.right_end.to_s)
    assert_equal(three.to_s, two.right_end.to_s)
    assert_equal(three.to_s, one.right_end.to_s)
  end

  def test_linked_list
    list = one = two = three = nil

    @koya.transaction do
      list = @koya.root['list'] = Koya::CellList.new
      one = list.push(ListCell.new('1'))
      assert_equal('1', one.to_s)
      two = list.push(ListCell.new('2'))
      assert_equal('2', two.to_s)
      three = two.insert_right(ListCell.new('3'))
      assert_equal('3', three.to_s)
    end

    assert_equal('1 2 3', list.collect {|x| x.to_s}.join(' '))

    list.transaction do
      list.delete(two)
      ary = list.collect { |x| x.to_s }
      assert_equal('1 3', ary.join(' '))

      list.unshift(two)
      ary = list.collect { |x| x.to_s }
      assert_equal('2 1 3', ary.join(' '))

      two.move_right
      ary = list.collect { |x| x.to_s }
      assert_equal('1 2 3', ary.join(' '))

      assert_equal('3', one.right_end.to_s)
    end

    list.transaction do
      right = list.pop
      assert_equal('3', right.to_s)
      ary = list.collect { |x| x.to_s }
      assert_equal('1 2', ary.join(' '))
    end

    list.transaction do
      left = list.shift
      assert_equal('1', left.to_s)
      assert_equal('2', list.collect { |x| x.to_s }.join(' '))
    end

    list.transaction do
      left = list.shift
      assert_equal('2', left.to_s)
      assert_equal('', list.collect { |x| x.to_s }.join(' '))

      left = list.shift
      assert_equal(nil, left)
      assert_equal('', list.collect { |x| x.to_s }.join(' '))
    end

    list.transaction do
      one = list.push(ListCell.new(1))
      (2...10).each do |n|
        a = list.push(ListCell.new(n))
      end
      assert_equal('123456789', list.collect { |x| x.to_s }.join(''))
    end

    list.transaction do
      one.move_right
      one.move_right
      one.move_right
      assert_equal('234156789', list.collect { |x| x.to_s }.join(''))
    end

    list.transaction do
      list.each do |x|
        assert_equal(x.koya_left.koya_right, x) if x.koya_left
        assert_equal(x.koya_right.koya_left, x) if x.koya_right
      end
    end

    list.transaction do
      list.unshift(list.pop)
      list.unshift(list.pop)
      list.unshift(list.pop)
      assert_equal('789234156', list.collect { |x| x.to_s }.join(''))
    end

    list.transaction do
      list.each do |x|
        assert_equal(x.koya_left.koya_right, x) if x.koya_left
        assert_equal(x.koya_right.koya_left, x) if x.koya_right
      end
    end

    list.transaction do
      list.push(list.shift)
      list.push(list.shift)
      list.push(list.shift)
      assert_equal('234156789', list.collect { |x| x.to_s }.join(''))
    end

    list.transaction do
      list.each do |x|
        assert_equal(x.koya_left.koya_right, x) if x.koya_left
        assert_equal(x.koya_right.koya_left, x) if x.koya_right
      end
    end
    
    list.transaction do
      one.move_left
      one.move_left
      one.move_left
      one.move_left
      assert_equal('123456789', list.collect { |x| x.to_s }.join(''))
    end

    rev = Time.now
    list.transaction do
      list.delete(one.koya_right)
    end

    list.transaction do
      assert_equal('13456789', list.collect { |x| x.to_s }.join(''))
    end

    @koya.revert_to(rev)
    list.transaction do
      assert_equal('123456789', list.collect { |x| x.to_s }.join(''))
    end

    list.transaction do
      list.pop
      assert_equal('12345678', list.collect { |x| x.to_s }.join(''))
    end

    @koya.gc

    list = @koya.root['list']
    list.transaction do
      assert_equal('12345678', list.collect { |x| x.to_s }.join(''))
    end

    rev = Time.now
    list.transaction do
      list.shift
      list.shift
    end

    list.transaction do
      assert_equal('345678', list.collect { |x| x.to_s }.join(''))
    end
    
    @koya.revert_to(rev)
    list.transaction do
      assert_equal('12345678', list.collect { |x| x.to_s }.join(''))
    end
  end

  class TwoCount < Koya::KoyaObject
    def initialize
      @count = 0
    end
    attr_reader :count

    def up_safe(succ=true)
      transaction do
        @count += 1
        raise 'Error' unless succ
        @count += 1
      end
    end

    def up(succ=true)
      @count += 1
      raise 'Error' unless succ
      @count += 1
    end
  end

  def test_abort_with_transaction
    root = @koya.root
    two = root.transaction { root['two'] = TwoCount.new }
    assert_equal(0, two.count)
    two.up
    assert_equal(2, two.count)
    root.transaction do
      begin
        two.up(false)
      rescue
      end
    end
    assert_equal(3, two.count)
    assert_raise(Koya::TransactionAborted) do
      root.transaction do
        begin
          two.up_safe(false)
        rescue
        end
      end
    end
    assert_equal(3, two.count)
  end

  def test_age
    root = @koya.root
    root.transaction do
      root['dict'] = Koya::Dict.new
      root['dict']['one'] = Koya::Dict.new
    end
    dict = root['dict']
    dict_id = dict._koya_rowid_
    assert_equal([], @koya.get_changed_prop(dict_id))

    dict['two'] = 2
    assert_equal([], @koya.get_changed_prop(dict_id))

    dict['one']['one-one'] = 11
    assert_equal(['one'], @koya.get_changed_prop(dict_id))

    @koya.touch_prop(dict_id, 'one')
    assert_equal([], @koya.get_changed_prop(dict_id))

    dict['two'] = dict['one']
    assert_equal([], @koya.get_changed_prop(dict_id))

    dict['one']['one-one'] = 22
    assert_equal(['one', 'two'], @koya.get_changed_prop(dict_id))

    @koya.touch_all_prop(dict_id)
    assert_equal([], @koya.get_changed_prop(dict_id))
  end
end
