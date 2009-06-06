# -*- indent-tabs-mode: nil -*-

require 'koya/koya'

class Koya
  class SQLiteStore < BasicStore
    begin
      require 'sqlite'
      Database = SQLite::Database
      SQLException = SQLite::Exceptions::SQLException
    rescue LoadError
      require 'sqlite3'
      Database = SQLite3::Database
      SQLException = SQLite3::SQLException
    end

    def initialize(name, use_log=true)
      super()
      @name = name
      @conn = Database.new(name)
      @conn.busy_handler {|a, b| on_busy(a, b) }
      @polling_interval = 0.1
      @in_transaction = nil
      @use_log = use_log
      @root = setup_root(use_log)
      setup_version
    end
    attr_reader :root, :version, :use_log, :in_transaction
    attr_accessor :polling_interval

    def on_busy(resource, retries)
      sleep(@polling_interval)
      true
    end

    def abort_transaction
      @abort = true
      @conn.rollback if @conn.transaction_active?
    end

    def transaction
      synchronize do
        if @in_transaction
          raise TransactionAborted if @abort
          return yield(@conn)
        end
        begin
          @revision = nil
          @abort = false
          stack = Thread.current['_koya_'] ||= []
          stack.push(self)
          @in_transaction = time_to_rev(Time.now)
          
          @conn.transaction
          abort = false
          begin
            @cache.begin
            result = yield(@conn)
            @cache.write_back
            update_revision(@conn) if @revision
            return result
          rescue Exception
            abort = true
            raise
          ensure
            if @conn.transaction_active?
              abort and @conn.rollback or @conn.commit
            end
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
        @conn.execute('vacuum') unless @in_transaction
      end
    end

    def setup_root(use_log = true)
      begin
        create_table(use_log)
      rescue SQLException
      end
      
      create_root
    end

    def create_table(use_log=true)
      transaction do |h|
        h.execute("create table version (koya, log, revision)")
        h.execute("insert into version (koya, log, revision) values (?, ?, ?)",
                  VERSION, use_log ? 1 : 0, 0.0)
        h.execute("create table memory (koya, klass, lock, gc, gen)")
        h.execute("create table prop (object, name, klass, value, time, unique(object, name))")
        h.execute("create table prop_log (object, name, klass, value, time, unique(object, name, time))")
        h.execute("create table koya_id (koya)")
        h.execute('insert into koya_id values(?)', "0")
      end
    end

    def get_koya_id
      transaction do |h|
        v = h.get_first_value('select koya from koya_id')
        v = (v.to_i + 1).to_s
        h.execute('update koya_id set koya=?', v)
        v
      end
    end

    def create_root
      transaction do |h|
        rowid = h.get_first_value('select koya from memory where klass=?',
                                  'root')
        return KoyaRef.new(self, rowid) if rowid

        return create_object('root')
      end
    end

    def create_object(klass)
      transaction do |h|
        rowid = get_koya_id
        h.execute("insert into memory (koya, klass, gen)  values (?, ?, 0)",
                  rowid, klass.to_s)
        KoyaRef.new(self, rowid)
      end
    end

    def lock_object(rowid)
      transaction do |h|
        locked = h.get_first_value("select lock from memory where koya=?",
                                   rowid)
        return false if locked
        h.execute("update memory set lock=1 where koya=?", rowid)
        return true
      end
    end

    def unlock_object(rowid)
      transaction do |h|
        h.execute("update memory set lock=null where koya=?", rowid)
      end
    end

    def get_klass(rowid)
      transaction do |h|
        h.get_first_value("select klass from memory where koya=?", rowid)
      end
    end

    def set_prop(rowid, name, obj)
      transaction do |h|
        k, v = dump_object(obj)
        h.execute("insert or replace into prop values (?, ?, ?, ?, ?)",
                  rowid, name.to_s, k, v, @in_transaction)
        h.execute("insert or replace into prop_log values (?, ?, ?, ?, ?)",
                  rowid, name.to_s, k, v, @in_transaction) if @use_log
        @revision = @in_transaction
        obj
      end
    end

    def get_prop(rowid, name)
      transaction do |h|
        k, v = h.get_first_row("select klass, value from prop where object=? and name=? and klass not null",
                               rowid, name.to_s)
        load_object(k, v)
      end
    end

    def update_revision(h)
      h.execute("update version set revision=?", @revision)
    end

    def delete_prop(rowid, name)
      transaction do |h|
        h.execute("insert or replace into prop values (?, ?, null, null, ?)",
                  rowid, name.to_s, @in_transaction)
        h.execute("insert or replace into prop_log values (?, ?, null, null, ?)",
                  rowid, name.to_s, @in_transaction) if @use_log
        @revision = @in_transaction
      end
    end

    def prop_keys(rowid)
      transaction do |h|
        h.execute("select name from prop where object=? and klass not null order by name",
                  rowid).collect {|k| k[0]}
      end
    end

    def prop_values(rowid)
      transaction do |h|
        h.execute("select name, klass, value from prop where object=? and klass not null",
                  rowid).collect do |v|
          load_object(v[1], v[2])
        end
      end
    end

    def all_prop(rowid)
      transaction do |h|
        h.execute("select name, klass, value from prop where object=? and klass not null",
                  rowid).collect do |v|
          [v[0], load_object(v[1], v[2])]
        end
      end
    end

    def all_ivars(rowid)
      transaction do |h|
        h.execute("select name, klass, value from prop where object=? and name like '@%' and klass not null",
                  rowid).collect do |v|
          [v[0], load_object(v[1], v[2])]
        end
      end
    end

    def extent(klass)
      name = klass.to_s
      synchronize do
        gc
        transaction do |h|
          h.execute("select koya from memory where klass=?",
                    name).collect do |r|
            load_object('@', r[0])
          end
        end
      end
    end

    def referer(rowid)
      synchronize do
        gc
        transaction do |h|
          h.execute("select object, name from prop where klass=? and value=?", 
                    '@', rowid).collect do |r|
            [load_object('@', r[0]), r[1]]
          end
        end
      end
    end

    def setup_version
      transaction do |h|
        @version, log = h.get_first_row("select koya, log from version")
        @use_log = (log.to_i > 0)
      end
    end

    def revert_to(time)
      @cache.clear
      raise CanNotRevertError unless @use_log
      rev = time_to_rev(time)
      transaction do |h|
        ary = h.execute("select object, name from prop where time>=?", rev)
        ary.each do |r|
          delete_prop(r[0], r[1])
          k, v = h.get_first_row("select klass, value from prop_log where time<=? and object=? and name=? order by time desc limit 1", rev, r[0], r[1])
          if k
            set_prop(r[0], r[1], load_object(k, v))
          end
        end
      end
    end

    def revision
      transaction do |h|
        t ,= h.get_first_row("select distinct revision from version")
        Time.at(t.to_f)
      end
    end

    def revisions
      transaction do |h|
        h.execute("select distinct time from prop_log order by time desc").collect do |r|
          Time.at(r[0].to_f)
        end
      end
    end

    def gc
      @cache.clear
      GC.new(self).gc
    end

    class GC
      def initialize(store)
        @store = store
      end

      def gc
        @store.transaction do |h|
          setup_mark
          level = 1
          while walk(level)
            level += 1
          end
          delete_unmarked
          delete_mark
          update_gen
        end
        @store.vacuum
      end

      private
      def setup_mark
        @store.transaction do |h|
          h.execute("update memory set gc=null")
          h.execute("update memory set gc=0 where lock not null or koya=?",
                    @store.root)
        end
      end

      def walk(level)
        sql_mark = <<EOQ
update memory set gc=? where gc is null and koya in (
  select value from prop where klass='@' and object in (
    select koya from memory where gc not null
  )
);
EOQ
        @store.transaction do |h|
          h.execute(sql_mark, level)
          count = h.get_first_value("select count(*) from memory where gc=?",
                                    level).to_i 
          return count > 0
        end
      end

      def delete_unmarked
        sql_delete_object = <<EOQ
delete from memory where gc is null;
EOQ
        sql_delete_prop = <<EOQ
delete from prop where object not in (
  select koya from memory where gc not null
);
EOQ
        @store.transaction do |h|
          h.execute(sql_delete_prop)
          h.execute(sql_delete_object)
        end
      end

      def delete_mark
        @store.transaction do |h|
          h.execute("update memory set gc=null")
          h.execute("delete from prop_log")
          h.execute("insert into prop_log select * from prop")
          h.execute("update prop_log set time=?", @store.in_transaction)
        end
      end

      def update_gen
        @store.transaction do |h|
          h.execute("update memory set gen=gen+1")
        end
      end
    end
  end
end
