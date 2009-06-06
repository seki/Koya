# -*- indent-tabs-mode: nil -*-

require 'koya/koya'
require 'koya/klass'
require 'villa'

=begin

== property

key = "p.#{rowid}@#{name}" 
val = Marshal.dump([klass, value])


== memory

key = "m.#{rowid}"
val = class.to_s

== log

key = "P.#{@in_transaction}@#{rowid}@#{name}"
val = Marshal.dump([klass, value])

== revision

key = "revision.#{@in_transaction}"
val = 1

== lock

key = "lock.#{rowid}"
val = 1
=end

class Koya
  class VillaStore < BasicStore
    def initialize(name, use_log=true)
      super()
      @name = name
      @in_transaction = nil
      @use_log = use_log
      @root = setup_root(use_log)
      @conn = nil
      setup_version
    end
    attr_reader :root, :version, :use_log, :in_transaction, :name
    attr_accessor :polling_interval

    def abort_transaction
      return if @abort
      begin
        @conn.tranabort 
      rescue Exception
      end
      begin
        @conn.close
      rescue Exception
      end
      @abort = true
    end

    def villa_transaction
      @revision = nil
      @abort = false
      @conn = Villa.new(@name, Villa::OWRITER | Villa::OCREAT)
      @conn.silent = true
      @conn.tranbegin
      return yield(@conn)
    rescue Exception
      @abort = true
      raise
    ensure
      begin
        @abort and @conn.tranabort or @conn.trancommit
      rescue Exception
      end
      begin
        @conn.close
      rescue Exception
      end
    end

    def transaction
      synchronize do
        if @in_transaction
          raise TransactionAborted if @abort
          return yield(@conn)
        end
        begin
          stack = Thread.current['_koya_'] ||= []
          stack.push(self)
          @in_transaction = time_to_rev(Time.now)

          villa_transaction do |h|
            @cache.begin
            result = yield(h)
            @cache.write_back
            h['revision'] = @revision if @revision
            return result
          end
        ensure
          stack.pop
          @cache.done
          @in_transaction = nil
        end
      end
    end

    def vacuum
      synchronize do
        Villa.new(@name, Villa::OWRITER | Villa::OCREAT) do |villa|
          villa.optimize
        end
      end
    end

    def setup_root(use_log=true)
      transaction do |v|
        if v['root']
          return KoyaRef.new(self, v['root'])
        end
        v['version'] = VERSION
        v['log'] = (use_log ? 1 : 0).to_s
        v['koya_id'] = '0'
        v['zzz'] = 'sentinel'
        v['revision'] = '0.0'
        root = create_object('root')
        v['root'] = root._koya_rowid_
        root
      end
    end

    def get_koya_id(h)
      rowid = (h['koya_id'].to_i + 1).to_s
      h['koya_id'] = rowid
    end

    def create_object(klass)
      transaction do |h|
        rowid = get_koya_id(h)
        h['m.' + rowid] = klass.to_s
        KoyaRef.new(self, rowid)
      end
    end

    def get_klass(rowid)
      transaction do |h|
        h['m.' + rowid]
      end
    end

    def prop_addr(rowid, name)
      "p.#{rowid}@#{name}"
    end

    def prop_log(h, rowid, name, k, v)
      return unless @use_log
      h["P.#{@in_transaction}@#{rowid}@#{name}"] = Marshal.dump([k, v])
      h['revision.' + @in_transaction] = '1'
      @revision = @in_transaction
    end

    def set_prop(rowid, name, obj)
      transaction do |h|
        k, v = dump_object(obj)
        h[prop_addr(rowid, name)] = Marshal.dump([k, v])
        prop_log(h, rowid, name, k, v)
        obj
      end
    end

    def get_prop(rowid, name)
      transaction do |h|
        begin
          value = h[prop_addr(rowid, name)]
          return nil unless value
          k, v = Marshal.load(value)
          return load_object(k, v)
        rescue
          return nil
        end
      end
    end

    def delete_prop(rowid, name)
      transaction do |h|
        h.delete(prop_addr(rowid, name))
        prop_log(h, rowid, name, nil, nil)
        @revision = @in_transaction
      end
    end

    def delete_object_wo_log(rowid)
      transaction do |h|
        h.delete("m.#{rowid}")
        prefix = "p.#{rowid}"
        h.curjump(prefix, Villa::JFORWARD)
        while h.curkey.index(prefix) == 0
          h.curout
        end
      end
    end

    def prop_loop(prefix)
      transaction do |h|
        h.curjump(prefix, Villa::JFORWARD)
        while h.curkey.index(prefix) == 0
          yield(h)
          h.curnext
        end
      end
    end

    def prop_keys(rowid)
      ary = []
      prefix = "p.#{rowid}@"
      prop_loop(prefix) do |h|
        key = h.curkey
        key[0, prefix.size] = ''
        ary.push(key)
      end
      ary
    end

    def prop_values(rowid)
      ary = []
      prefix = "p.#{rowid}@"
      prop_loop(prefix) do |h|
        k, v = Marshal.load(h.curval)
        ary.push(load_object(k, v))
      end
      ary
    end

    def all_prop(rowid)
      ary = []
      prefix = "p.#{rowid}@"
      prop_loop(prefix) do |h|
        key = h.curkey
        key[0, prefix.size] = ''
        k, v = Marshal.load(h.curval)
        ary.push([key, load_object(k, v)])
      end
      ary
    end

    def all_ivars(rowid)
      ary = []
      prefix = "p.#{rowid}@@"
      prop_loop(prefix) do |h|
        key = h.curkey
        key[0, prefix.size - 1] = ''
        k, v = Marshal.load(h.curval)
        ary.push([key, load_object(k, v)])
      end
      ary
    end

    def lock_object(rowid)
      addr = 'lock.' + rowid
      transaction do |h|
        return false if h[addr]
        h[addr] = '1'
        true
      end
    end

    def unlock_object(rowid)
      addr = 'lock.' + rowid
      transaction do |h|
        h.delete(addr)
      end
    end

    def each_prop(h)
      prefix = 'p.'
      h.curjump(prefix, Villa::JFORWARD)
      while /^p\.(\d+)\@(.+)$/ =~ h.curkey
        yield($1, $2, *Marshal.load(h.curval))
        h.curnext
      end
    end

    def each_object(h)
      prefix = 'm.'
      h.curjump(prefix, Villa::JFORWARD)
      while /^m\.(\d+)$/ =~ h.curkey
        yield($1, h.curval)
        h.curnext
      end
    end

    def extent(klass)
      synchronize do
        gc
        transaction do |h|
          ary = []
          each_object(h) do |rowid, k|
            if k == klass.to_s
              ary << load_object('@', rowid)
            end
          end
          return ary
        end
      end
    end

    def referer(rowid)
      synchronize do
        gc
        value = Marshal.dump(['@', rowid])
        transaction do |h|
          ary = []
          prefix = 'p.'
          prop_loop(prefix) do
            next unless value == h.curval
            if /^p\.(\d+)\@(.+)$/ =~ h.curkey
              ary << [load_object('@', $1), $2]
            end
          end
          return ary
        end
      end
    end

    def setup_version
      transaction do |v|
        @version = v['version']
        @use_log = (v['log'].to_i > 0)
      end
    end

    def revert_to(time)
      @cache.clear
      raise CanNotRevertError unless @use_log
      rev = time_to_rev(time)
      latest = LatestProp.new(self)
      latest.update(rev)
    end

    def revision
      transaction do |h|
        Time.at(h['revision'].to_f)
      end
    end

    def revisions
      ary = []
      transaction do |h|
        h.curjump('revision.', Villa::JFORWARD)
        while /^revision\.(.+)$/ =~ h.curkey
          ary.unshift(Time.at($1.to_f))
          h.curnext
        end
      end
      ary
    end

    def gc
      @cache.clear
      GC.new(self).gc
    end
    
    class LatestProp
      def initialize(store)
        @store = store
        @name = @store.name + '.rev'
      end

      def update(rev)
        Villa.new(@name, Villa::OWRITER | Villa::OCREAT) do |villa|
          @store.transaction do |h|
            flatten(rev, villa, h)
            collect_deleted(villa, h)
            restore(villa, h)
          end
          villa.clear
        end
        File::unlink(@name)
      end

      def flatten(rev, villa, h)
        prefix = "P.#{rev}@"
        h.curjump(prefix, Villa::JBACKWARD)
        while /^P\.\d+\.\d+\@(\d+)\@(.+)$/ =~ h.curkey
          key = "p.#{$1}@#{$2}"
          villa[key] = h.curval unless villa[key]
          h.curprev
        end
      end

      def collect_deleted(villa, h)
        prefix = 'p.'
        h.curjump(prefix, Villa::JFORWARD)
        while h.curkey.index(prefix) == 0
          unless villa[h.curkey]
            villa[h.curkey] = Marshal.dump([nil, nil])
          end
          h.curnext
        end
      end
      
      def restore(villa, h)
        villa['zzz'] = 'sentinel'
        prefix = 'p.'
        villa.curjump(prefix, Villa::JFORWARD)
        while /^p\.(\d+)\@(.+)$/ =~ villa.curkey
          rowid = $1
          name = $2
          k, v = Marshal.load(villa.curval)
          if k
            h[villa.curkey] = Marshal.dump([k, v])
            @store.prop_log(h, rowid, name, k, v)
          else
            @store.delete_prop(rowid, name)
          end
          villa.curnext
        end
      end
    end

    class GC
      def initialize(store)
        @store = store
        @name = @store.name + '.gc'
        @footprint = {}
      end

      def gc
        Villa.new(@name, Villa::OWRITER | Villa::OCREAT) do |villa|
          @villa = villa
          @villa.silent = true
          @store.transaction do |h|
            setup_mark
            while walk 
              ; 
            end
            delete_unmarked
            delete_prop_log
          end
          @villa.clear
        end
        @store.vacuum
        File::unlink(@name)
      end

      private
      def footprint(rowid)
        if @villa[rowid]
          @footprint[rowid] = true 
          @villa.delete(rowid)
        end
      end

      def setup_mark
        @villa.clear
        @footprint = {}
        @store.transaction do |h|
          prefix = 'm.'
          h.curjump(prefix, Villa::JFORWARD)
          while /^m\.(\d+)$/ =~ h.curkey
            @villa[$1] = '1'
            h.curnext
          end
        end
        footprint(@store.root.to_s)
        @store.transaction do |h|
          prefix = 'lock.'
          h.curjump(prefix, Villa::JFORWARD)
          while /^lock\.(\d+)$/ =~ h.curkey
            footprint($1)
            h.curnext
          end
        end
      end

      def walk
        keys = @footprint.keys
        @footprint = {}
        @store.transaction do |h|
          keys.each do |rowid|
            prefix = "p.#{rowid}@"
            h.curjump(prefix, Villa::JFORWARD)
            while h.curkey.index(prefix) == 0
              k, v = Marshal.load(h.curval)
              footprint(v) if k == '@'
              h.curnext
            end
          end
        end

        return @footprint.size > 0
      end

      def delete_unmarked
        @store.transaction do |h|
          @villa.each do |k, v|
            @store.delete_object_wo_log(k)
          end
        end
      end

      def delete_prop_log
        return unless @store.use_log
        @store.transaction do |h|
          prefix = 'P.'
          h.curjump(prefix, Villa::JFORWARD)
          while h.curkey.index(prefix) == 0
            h.curout
          end
          
          prefix = 'p.'
          h.curjump(prefix, Villa::JFORWARD)
          while /^p\.(.+)$/ =~ h.curkey
            @villa["P.#{@store.in_transaction}@#{$1}"] = h.curval
            h.curnext
          end

          prefix = 'P.'
          @villa.curjump(prefix, Villa::JFORWARD)
          while @villa.curkey.to_s.index(prefix) == 0
            h[@villa.curkey] = @villa.curval
            @villa.curnext
          end
        end
      end
    end
  end
end
