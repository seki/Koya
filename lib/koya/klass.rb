# -*- indent-tabs-mode: nil -*-

require 'koya/koya'

class Koya
  module DictMixin
    def [](key)
      @_koya_cache_[key.to_s]
    end

    def []=(key, value)
      @_koya_cache_[key.to_s] = @_koya_[key.to_s] = value
    end
    
    def each(&block)
      @_koya_cache_.each(&block)
    end

    def keys
      @_koya_cache_.keys
    end

    def values
      @_koya_cache_.values
    end
  end

  class Dict < KoyaObject
    include Enumerable
    include DictMixin
  end

  class Stream < KoyaObject
    include Enumerable
    def initialize
      @head = 0
      @size = 0
    end
    attr_accessor(:head)
    attr_accessor(:size)

    def [](key, length=nil)
      transaction do
        range_or_key = to_range(key, length)

        if Range === range_or_key
          range_or_key.collect do |k|
            index = to_index(@head, @size, k)
            index ? fetch(index) : nil
          end
        elsif Integer === range_or_key
          index = to_index(@head, @size, range_or_key)
          index ? fetch(index) : nil
        else
          nil
        end
      end
    end

    def push(value)
      transaction do
        store(@head + @size, value)
        @size += 1
      end
    end

    def <<(value)
      push(value)
    end

    def unshift(value)
      transaction do
        @head -= 1
        store(@head, value)
        @size += 1
      end
    end

    def pop
      transaction do
        return nil if @size <= 0
        @size -= 1
        fetch(@head + @size, true)
      end
    end
    
    def shift
      transaction do
        return nil if @size <= 0
        @head += 1
        @size -= 1
        fetch(@head - 1, true)
      end
    end
    
    def to_a
      transaction do
        (@head...(@head + @size)).inject([]) do |ary, i|
          ary << fetch(i)
        end
      end
    end

    def replace(ary)
      transaction do
        old_size = @size
        
        ary.each do |x|
          self.push(x)
        end

        @head += old_size
        @size -= old_size

        self
      end
    end

    def each(&block)
      # to_a.each(&block)
      each_in_transaction(&block)
    end

    def each_in_transaction(&block)
      transaction do
        (@head...(@head + @size)).each do |i|
          yield(fetch(i))
        end
      end
    end

    private
    def to_range(key, length)
      if length
        return nil if length < 0
        key...(length + key)
      elsif Range === key
        key
      else
        key
      end
    end

    def to_index(head, size, key)
      key = size + key if key < 0

      if (0...size) === key
        head + key
      else
        false
      end
    end

    def store(index, value)
      @_koya_[index] = value
    end
    
    def fetch(index, remove=false)
      @_koya_[index]
    ensure
      @_koya_.delete(index) if remove
    end
  end

  class Set < KoyaObject
    include Enumerable

    def add(koya_obj)
      key = koya_obj._koya_.to_s
      @_koya_cache_[key] = @_koya_[key] = koya_obj
      key
    end
    alias push add
    
    def <<(koya_obj)
      add(koya_obj)
    end

    def delete(key_or_obj)
      key = nil
      begin
        key = key_or_obj._koya_.to_s
      rescue
        key = key_or_obj.to_s
      end
      @_koya_.delete(key)
      @_koya_cache_.delete(key)
    end

    def include?(key_or_obj)
      key = nil
      begin
        key = key_or_obj._koya_.to_s
      rescue
        key = key_or_obj.to_s
      end
      @_koya_cache_[key] ? true : false
    end

    def [](key)
      @_koya_cache_[key]
    end

    def to_a
      @_koya_cache_.values
    end

    def each(&block)
      to_a.each(&block)
    end
  end

  class Cell < KoyaObject
    include Enumerable

    attr_accessor :koya_left, :koya_right

    def insert_right(it)
      transaction do
        it.koya_left = self
        it.koya_right = @koya_right
        it.koya_right.koya_left = it if it.koya_right
        @koya_right = it
      end
      it
    end

    def insert_left(it)
      transaction do
        it.koya_right = self
        it.koya_left = @koya_left
        it.koya_left.koya_right = it if it.koya_left
        @koya_left = it
      end
      it
    end

    def detach
      transaction do
        left = @koya_left
        right = @koya_right
        left.koya_right = right if left
        right.koya_left = left if right
        @koya_left = nil
        @koya_right = nil
      end
      self
    end

    def move_right
      transaction do
        right = @koya_right
        if right
          right.detach
          insert_left(right)
        end
      end
      self
    end

    def move_left
      transaction do
        left = @koya_left
        if left
          left.detach
          insert_right(left)
        end
      end
      self
    end

    def each
      transaction do
        yield(self)
        curr = @koya_right
        while curr
          break if curr == self
          yield(curr)
          curr = curr.koya_right
        end
      end
    end

    def right_end
      curr = self
      while (right = curr.koya_right)
        curr = right
      end
      return curr
    end

    def left_end
      curr = self
      while (left = curr.koya_left)
        curr = left
      end
      return curr
    end
  end

  class CellList < KoyaObject
    include Enumerable
    attr_accessor :pivot_right, :pivot_left
    
    def unshift(cell)
      transaction do
        left = @pivot_left || @pivot_right
        if left
          left.left_end.insert_left(cell)
          @pivot_left = cell
        else
          @pivot_left = cell
          @pivot_right = cell
        end
        return cell
      end
    end

    def push(cell)
      transaction do
        right = @pivot_right || @pivot_left
        if right
          right.right_end.insert_right(cell)
          @pivot_right = cell
        else
          @pivot_left = cell
          @pivot_right = cell
        end
        return cell
      end
    end

    def shift
      transaction do
        left = @pivot_left || @pivot_right
        return nil unless left
        left = left.left_end
        delete(left)
        return left
      end
    end

    def pop
      transaction do
        right = @pivot_right || @pivot_left
        return nil unless right
        right = right.right_end
        delete(right)
        return right
      end
    end

    def delete(cell)
      transaction do
        if @pivot_right == cell
          @pivot_right = cell.koya_left
        end
        if @pivot_left == cell
          @pivot_left = cell.koya_right
        end
        cell.detach
      end
      return cell
    end

    def each(&block)
      transaction do
        left = @pivot_left || @pivot_right
        return self unless left
        if left.koya_left
          left = left.left_end
          @pivot_left = left
        end
        left.each(&block)
      end
    end
  end

  class OrderedSet < KoyaObject
    include Enumerable
    def initialize(other=nil)
      return unless other
      other.each do |x|
        push(x)
      end
    end

    def order(kobj)
      kobj._koya_.rowid
    end
    
    def push(kobj)
      @_koya_[order(kobj)] = kobj
    end
    alias add push
    alias << push

    def keys
      @_koya_.keys
    end

    def each
      @_koya_.keys.each do |x|
        yield(@_koya_[x])
      end
    end
    
    def delete(kobj)
      @_koya_.delete(order(kobj))
    end
  end
end


