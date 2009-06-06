# -*- coding: utf-8 -*-
# -*- indent-tabs-mode: nil -*-

require 'koya/koya'
require 'koya/klass'
require 'tokyocabinet'

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
  class TokoyaStore < BasicStore
    class BDBError < RuntimeError
      def initialize(bdb)
        super(bdb.errmsg(bdb.ecode))
      end
    end
    
    class BDB < TokyoCabinet::BDB
      def exception
        BDBError.new(self)
      end
      
      def cursor
        TokyoCabinet::BDBCUR.new(self)
      end
      
      def self.call_or_die(*ary)
        file, lineno = __FILE__, __LINE__
        if /^(.+?):(¥d+)(?::in `(.*)')?/ =~ caller(1)[0]
          file = $1
          lineno = $2.to_i
        end
        ary.each do |sym|
          module_eval("def #{sym}(*arg); super || raise(self); end",
                      file, lineno)
        end
      end
      
      call_or_die :open, :close
      call_or_die :tranbegin, :tranabort, :trancommit
      call_or_die :vanish
    end

    def initialize(name, use_log=true)
      super()
      @name = name
      @conn = BDB.new
      @in_transaction = nil
      @use_log = use_log
      @root = setup_root(use_log)
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

    def tokoya_transaction
      @revision = nil
      @abort = false
      @conn.open(@name, TokyoCabinet::BDB::OWRITER | TokyoCabinet::BDB::OCREAT)
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

          tokoya_transaction do |h|
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
      # NOP
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
        h.outlist(prop_addr(rowid, name))
        prop_log(h, rowid, name, nil, nil)
        @revision = @in_transaction
      end
    end

    def delete_object_wo_log(rowid)
      transaction do |h|
        h.outlist("m.#{rowid}")
        prefix = "p.#{rowid}"
        cursor = h.cursor
        cursor.jump(prefix)
        while cursor.key.index(prefix) == 0
          cursor.out
        end
      end
    end

    def prop_loop(prefix)
      transaction do |h|
        cursor = h.cursor
        cursor.jump(prefix)
        while cursor.key.index(prefix) == 0
          yield(cursor)
          cursor.next
        end
      end
    end

    def prop_keys(rowid)
      ary = []
      prefix = "p.#{rowid}@"
      prop_loop(prefix) do |h|
        key = cursor.key
        key[0, prefix.size] = ''
        ary.push(key)
      end
      ary
    end

    def prop_values(rowid)
      ary = []
      prefix = "p.#{rowid}@"
      prop_loop(prefix) do |cursor|
        k, v = Marshal.load(cursor.val)
        ary.push(load_object(k, v))
      end
      ary
    end

    def all_prop(rowid)
      ary = []
      prefix = "p.#{rowid}@"
      prop_loop(prefix) do |cursor|
        key = cursor.key
        key[0, prefix.size] = ''
        k, v = Marshal.load(cursor.val)
        ary.push([key, load_object(k, v)])
      end
      ary
    end

    def all_ivars(rowid)
      ary = []
      prefix = "p.#{rowid}@@"
      prop_loop(prefix) do |cursor|
        key = cursor.key
        key[0, prefix.size - 1] = ''
        k, v = Marshal.load(cursor.val)
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
        h.outlist(addr)
      end
    end

    def each_prop(h)
      prefix = 'p.'
      cursor = h.cursor
      cursor.jump(prefix)
      while /^p\.(\d+)\@(.+)$/ =~ cursor.key
        yield($1, $2, *Marshal.load(cursor.val))
        cursor.next
      end
    end

    def each_object(h)
      prefix = 'm.'
      cursor = h.cursor
      cursor.jump(prefix)
      while /^m\.(\d+)$/ =~ cursor.key
        yield($1, cursor.val)
        cursor.next
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
          prop_loop(prefix) do |cursor|
            next unless value == cursor.val
            if /^p\.(\d+)\@(.+)$/ =~ cursor.key
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
        cursor = h.cursor
        cursor.jump('revision.')
        while /^revision\.(.+)$/ =~ cursor.key
          ary.unshift(Time.at($1.to_f))
          cursor.next
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
        work = BDB.new
        work.open(@name,
                  TokyoCabinet::BDB::OWRITER | TokyoCabinet::BDB::OCREAT)
        begin
          @store.transaction do |h|
            flatten(rev, work, h)
            collect_deleted(work, h)
            restore(work, h)
          end
          work.vanish
        ensure
          work.close
          File::unlink(@name) rescue nil
        end
      end

      def flatten(rev, work, h)
        cursor = h.cursor
        cursor.jump("revision.#{rev}")
        unless /^revision\.(.+)$/ =~ cursor.key
          return # fixme
        end
        prefix = "P.#{$1}@"
        cursor.jump(prefix)
        cursor.prev
        while /^P\.\d+\.\d+\@(\d+)\@(.+)$/ =~ cursor.key
          key = "p.#{$1}@#{$2}"
          work[key] = cursor.val unless work[key]
          break unless cursor.prev
        end
      end

      def collect_deleted(work, h)
        prefix = 'p.'
        cursor = h.cursor
        cursor.jump(prefix)
        while cursor.key.index(prefix) == 0
          unless work[cursor.key]
            work[cursor.key] = Marshal.dump([nil, nil])
          end
          cursor.next
        end
      end
      
      def restore(work, h)
        work['zzz'] = 'sentinel'
        prefix = 'p.'
        cursor = work.cursor
        cursor.jump(prefix)
        while /^p\.(\d+)\@(.+)$/ =~ cursor.key
          rowid = $1
          name = $2
          k, v = Marshal.load(cursor.val)
          if k
            h[cursor.key] = Marshal.dump([k, v])
            @store.prop_log(h, rowid, name, k, v)
          else
            @store.delete_prop(rowid, name)
          end
          cursor.next
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
        @work = BDB.new
        @work.open(@name,
                   TokyoCabinet::BDB::OWRITER | TokyoCabinet::BDB::OCREAT)
        begin
          @store.transaction do |h|
            setup_mark
            while walk 
              ; 
            end
            delete_unmarked
            delete_prop_log
          end
          @work.vanish
          @store.vacuum
        ensure
          @work.close
          File::unlink(@name)
        end
      end

      private
      def footprint(rowid)
        if @work[rowid]
          @footprint[rowid] = true 
          @work.delete(rowid)
        end
      end

      def setup_mark
        @work.clear
        @footprint = {}
        @store.transaction do |h|
          prefix = 'm.'
          cursor = h.cursor
          cursor.jump(prefix)
          while /^m\.(\d+)$/ =~ cursor.key
            @work[$1] = '1'
            cursor.next
          end
        end
        footprint(@store.root.to_s)
        @store.transaction do |h|
          prefix = 'lock.'
          cursor = h.cursor
          cursor.jump(prefix)
          while /^lock\.(\d+)$/ =~ cursor.key
            footprint($1)
            cursor.next
          end
        end
      end

      def walk
        keys = @footprint.keys
        @footprint = {}
        @store.transaction do |h|
          keys.each do |rowid|
            prefix = "p.#{rowid}@"
            cursor = h.cursor
            cursor.jump(prefix)
            while cursor.key.index(prefix) == 0
              k, v = Marshal.load(cursor.val)
              footprint(v) if k == '@'
              cursor.next
            end
          end
        end

        return @footprint.size > 0
      end

      def delete_unmarked
        @store.transaction do |h|
          @work.each do |k, v|
            @store.delete_object_wo_log(k)
          end
        end
      end

      def delete_prop_log
        return unless @store.use_log
        @store.transaction do |h|
          prefix = 'P.'
          cursor = h.cursor
          cursor.jump(prefix)
          while cursor.key.index(prefix) == 0
            cursor.out
          end
          
          prefix = 'p.'
          cursor.jump(prefix)
          while /^p\.(.+)$/ =~ cursor.key
            @work["P.#{@store.in_transaction}@#{$1}"] = cursor.val
            cursor.next
          end

          prefix = 'P.'
          cursor = @work.cursor
          cursor.jump(prefix)
          while cursor.key.to_s.index(prefix) == 0
            h[cursor.key] = cursor.val
            cursor.next
          end
        end
      end
    end
  end
end
