require 'sqlite'

class Updater
  def initialize(name)
    @conn = SQLite::Database.new(name)
  end

  def check_version
    @conn.transaction do |h|
      @version, log = h.get_first_row('select koya, log from version')
      raise('invalid version: ' + @version.inspect) if @version != "0.1"
      h.execute("update version set koya = ?", '0.2')
    end
  end

  def update_memory
    @conn.transaction do |h|
      h.execute('create table memory2 (koya, klass, lock, gc, gen)')
      h.execute('insert into memory2 select rowid, * from memory')
      h.execute('drop table memory')
      h.execute('create table memory (koya, klass, lock, gc, gen)')
      h.execute('insert into memory select * from memory2')
      h.execute('drop table memory2')
    end
  end

  def update_seq
    @conn.transaction do |h|
      value ,= h.get_first_row('select max(koya) from memory')
      h.execute('create table koya_id (koya)')
      h.execute('insert into koya_id values(?)', value)
    end
  end
end

u = Updater.new(ARGV.shift)
u.check_version
u.update_memory
u.update_seq

