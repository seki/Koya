# -*- indent-tabs-mode: nil -*-

require 'monitor'

class Koya
  VERSION = '0.4'

  class KoyaError < RuntimeError; end
  class TransactionNotFound < KoyaError; end
  class TransactionAborted < KoyaError; end
  class CanNotRevertError < RuntimeError; end
  class ObjectNotFound < KoyaError; end

  class Cache
    def initialize(store, gcache = true)
      @store = store
      @use_global = gcache
      @global = {}
      @context = {}
      @rev = nil
    end

    def clear
      @global = {}
      @context = {}
    end

    def begin
      if @use_global && @global.size > 0
        rev = @store.revision
        if @rev != rev
          @global = {}
        end
      end
      @context = {}
    end

    def done
      @context.each {|k, v| v._koya_leave_ }
      @context = {}
    end

    def []=(rowid, object)
      @context[rowid] = object
    end

    def [](rowid)
      if @global[rowid]
        @context[rowid] = @global[rowid]
        @global[rowid] = nil
      end
      @context[rowid]
    end

    def write_back
      @context.each do |k, v|
        v._koya_store_ivars_
        if @use_global
          @global[k] = v._koya_clone_
        end
      end
      if @use_global
        @rev = @store.revision
      end
    end
  end

  class BasicStore
    include MonitorMixin
    def initialize
      super()
      @cache = Cache.new(self)
    end
    attr_reader :cache
    
    def time_to_rev(time)
      sprintf("%020.8f", time)
    end

    def dump_object(obj)
      case obj
      when KoyaObject
        return '@', obj._koya_.to_s
      when KoyaRef
        return '@', obj._koya_rowid_.to_s
      when Fixnum
        return 'i', obj.to_i
      when String
        return 's', obj.to_s
      else
        begin
          return 'r', Marshal.dump(obj).dump
        rescue
          return 'R', obj.inspect
        end
      end
    end

    def load_object(klass, value)
      case klass
      when '@'
        KoyaRef.new(self, value).freeze
      when 'i'
        value.to_i
      when 's'
        value.to_s.freeze
      when 'r'
        Marshal.load(eval(value)).freeze
      when 'R'
        value.to_s
      else
        nil
      end
    end

    def [](name)
      @root[name]
    end

    def []=(name, obj)
      @root[name] = obj
    end
  end

  class KoyaObject
    def self.bless(ref)
      store = Thread.current['_koya_'].to_a[-1]
      raise TransactionNotFound unless store
      raise TransactionNotFound unless store == ref._koya_store_

      rowid = ref._koya_rowid_
      return store.cache[rowid] if store.cache[rowid]

      it = self.allocate
      it.instance_eval {
        @_koya_ = KoyaStuff.new(ref)
        @_koya_ref_ = ref
        @_koya_cache_ = {}
        store.all_prop(rowid).each do |name, value|
          @_koya_cache_[name] = value
          next if /^@_/ =~ name
          next unless /^@/ =~ name
          # @_koya_cache_[name] = value
          instance_variable_set(name, value)
        end
      }
      store.cache[rowid] = it
      it
    end
    
    def self.new(*arg)
      store = Thread.current['_koya_'].to_a[-1]
      raise TransactionNotFound unless store
      ref = store.create_object(self)
      it = bless(ref)
      it.instance_eval {
        initialize(*arg)
      }
      it
      ref
    end

    def transaction
      @_koya_.store.transaction do |koya|
        begin
          yield(koya)
        rescue Exception
          @_koya_.store.abort_transaction
          raise
        end
      end
    end
    
    attr_reader :_koya_
    attr_reader :_koya_ref_

    def ==(other)
      @_koya_ref_ == other._koya_ref_
    rescue NoMethodError
      false
    end

    def _koya_store_ivars_
      instance_variables.each do |name|
        value = instance_variable_get(name)
        next if /^@_/ =~ name
        next if @_koya_cache_[name] == value
        @_koya_[name] = value
        if value.kind_of?(KoyaObject)
          value = value._koya_ref_
        end
        @_koya_cache_[name] = value
      end
    end

    def _koya_leave_
      ary = methods.find_all {|x| 
        not ['__id__', '__send__', '_koya_ref_', '_koya_clone_'].include?(x)
      }

      eval("class << self; undef_method(:#{ary.join(', :')}); def method_missing(m, *a, &b); @_koya_ref_.__send__(m, *a, &b); end; end")
    end

    def _koya_clone_
      self.class._koya_clone_(@_koya_ref_, @_koya_cache_)
    end

    def self._koya_clone_(ref, cache)
      it = self.allocate
      it.instance_eval {
        @_koya_ = KoyaStuff.new(ref)
        @_koya_ref_ = ref
        @_koya_cache_ = cache
        cache.each do |name, value|
          next if /^@_/ =~ name
          next unless /^@/ =~ name
          instance_variable_set(name, value) 
        end
      }
      it
    end
  end

  class KoyaRoot < KoyaObject
    def store
      @_koya_.store
    end

    def [](key)
      @_koya_[key]
    end

    def []=(key, value)
      @_koya_[key] = value
    end

    def delete(key)
      @_koya_.delete(key)
    end

    def to_s
      @_koya_.to_s
    end
  end

  class KoyaStuff
    def initialize(ref)
      @ref = ref
      @store = ref._koya_store_
      @rowid = ref._koya_rowid_
    end
    attr_reader :rowid, :store, :ref

    def ==(other)
      self.class == other.class &&
      self.store == other.store &&
      self.rowid == other.rowid
    end

    def to_s
      @rowid.to_s
    end

    def [](key)
      @store.get_prop(@rowid, key)
    end

    def []=(key, value)
      @store.set_prop(@rowid, key, value)
    end

    def delete(key)
      @store.delete_prop(@rowid, key)
    end

    def keys
      @store.prop_keys(@rowid)
    end

    def values
      @store.prop_values(@rowid)
    end

    def each(&block)
      @store.transaction do
        @store.all_prop(@rowid).each(&block)
      end
    end

    def to_a
      @store.transaction do
        @store.all_prop(@rowid)
      end
    end

    def lock
      @store.lock_object(@rowid)
    end

    def unlock
      @store.unlock_object(@rowid)
    end
  end

  class KoyaRef
    def initialize(store, rowid)
      @store = store
      @rowid = rowid
    end

    def ==(other)
      begin
        if self.class != other.class
          other = other._koya_ref_
        end

        self.class == other.class &&
        @store == other._koya_store_ &&
        @rowid == other._koya_rowid_
      rescue NoMethodError
        false
      end
    end

    def _koya_bless_
      klass = @store.get_klass(@rowid)
      case klass
      when '@'
        self
      when nil
        raise ObjectNotFound
      when 'root'
        KoyaRoot.bless(self)
      else
        klass.split(/::/).inject(Object) { |c, n| c.const_get(n) }.bless(self)
      end
    end
    private :_koya_bless_

    def _koya_rowid_; @rowid; end
    def _koya_store_; @store; end

    def method_missing(msg_id, *a, &b)
      if Thread.current['_koya_'].to_a[-1]
        _koya_bless_.__send__(msg_id, *a, &b)
      else
        @store.transaction { _koya_bless_.__send__(msg_id, *a, &b) }
      end
    end

    undef :to_a
    undef :to_s
  end
end
