require 'fileutils'
require 'concurrent'
require 'thread'

module Oxygene
    OPERATIONS = {
        :set => 1,
        :add => 2,
        :delete => 3
    }

    # ------------------------------------
    # database
    # ------------------------------------
    class DB
        attr_accessor :path

        def initialize(path, connection_pool)
            @path = path
            @logs = []

            # the db is a folder containing log and data files
            FileUtils.mkdir_p(@path)

            # FIXME: ensure connection_pool > num threads
            # connection pool
            @connection_mutex = Mutex.new
            @connections = connection_pool.times.collect do |i|
                Connection.new(self, i)
            end

            # row/column level locking
            @locking_mutex = Mutex.new
            @mutexes = {}
        end

        def get_connection
            @connection_mutex.synchronize do
                @connections.find(&:available).start
            end
        end

        def mutex_on(name)
            @locking_mutex.synchronize do
                @mutexes[name] ||= Mutex.new
            end
        end

        def close
            @connections.each(&:close)
        end
    end

    class Connection
        attr_reader :db, :id, :available

        # all writes made through a connection go to the same log file. reads
        # are serviced through all logs, then data files.
        def initialize(db, id)
            @db = db
            @id = id
            @available = true
            @log = Log.new(self)
        end

        def close
            @log.close
        end

        # use/complete cycle for connections
        def start
            @available = false
            self
        end

        def finish
            @available = true
        end
    end

    # ------------------------------------
    # on disk and in memory indexes
    # ------------------------------------
    class Log
        def initialize(connection)
            @path  = File.join(connection.db.path, "#{connection.id}.log")
            @lock  = Concurrent::ReadWriteLock.new
            @valid = true

            create_or_load
            connection.db.logs << self
        end

        def create_or_load
            if File.exists?(@path)
                @file = File.open(@path, 'r+b')
            else
                @file = File.open(@path, 'w+b')
                @size = 0
            end

            @open = true
        end

        def close
            @lock.with_write_lock do
                @file.close
            end
        end

        def flush
            @lock.with_write_lock do
                @file.fsync
            end
        end

        def add(transaction)
            str = transaction.to_s
            str_length = str.length

            @lock.with_write_lock do
                next false unless @valid && @open

                # if an exception occurs during the write, or the write was
                # incomplete @valid becomes false, otherwise return true
                begin
                    written = @file.write(str)
                    if written == str_length
                        @size += str_length
                        next true
                    end
                rescue
                end

                @valid = false
            end
        end
    end

    # ------------------------------------
    # data and operations
    # ------------------------------------
    class Key
        attr_accessor :table, :row, :column, :qualifier, :timestamp
        KEY_FORMAT = 'ZQ>ZZQ>'

        def initialize(table, row, column, qualifier = nil, timestamp = nil)
            @table = table
            @row = row
            @column = column
            @qualifier = qualifier
            @timestamp = timestamp || Time.now.nsec
        end

        def to_s
            [@table, @row, @column, @qualifier, @timestamp].pack(KEY_FORMAT)
        end

        def self.from_s(str)
           table, row, column, qualifier, timestamp = str.unpack(KEY_FORMAT)
           qualifier = nil if qualifier.empty?
           Key.new(table, row, column, qualifier, timestamp)
        end
    end

    class Transaction
        def initialize
            @operations = []
        end

        def set(key, value)
            @operations << [key, :set, value]
        end

        def add(key, value)
            @operations << [key, :add, value]
        end

        def delete(key)
            @operations << [key, :delete]
        end

        def to_s

        end

        def self.from_file(file)
            
        end
    end
end
