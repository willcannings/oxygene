# encoding: ASCII-8BIT
require 'fileutils'
require 'concurrent'
require 'thread'

module Oxygene
    # ------------------------------------
    # database
    # ------------------------------------
    class DB
        attr_accessor :path, :logs

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

        def transaction
            Transaction.new(@log)
        end
    end

    # ------------------------------------
    # on disk and in memory indexes
    # ------------------------------------
    class Log
        attr_reader :file, :valid

        def initialize(connection)
            @path  = File.join(connection.db.path, "#{connection.id}.log")
            @lock  = Concurrent::ReadWriteLock.new
            @valid = true

            create_or_load
            connection.db.logs << self
        end

        # on disk operations
        def create_or_load
            if File.exists?(@path)
                @file = File.open(@path, 'r+b')
                @size = @file.size
            else
                @file = File.open(@path, 'w+b')
                @size = 0
            end

            while @file.pos < @size
                transaction = Transaction.from_log(self)
                merge_transaction(transaction)
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

        # mutating the log
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
                        merge_transaction(transaction)
                        @size += str_length
                        next true
                    end
                rescue
                end

                @valid = false
            end
        end

        def merge_transaction(transaction)
            p transaction.operations
        end
    end

    # ------------------------------------
    # data and operations
    # ------------------------------------
    class Key
        attr_accessor :table, :row, :column, :qualifier, :timestamp
        KEY_FORMAT = 'Z*Q>Z*Z*Q>'

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

    class Value
        attr_reader :value
        def initialize(value)
            @value = value
        end

        def self.from_s(str)
            Value.new(str)
        end
    end

    class Operation
        OPERATION_MAGIC  = "\0"
        OPERATION_HEADER_FORMAT = 'aCL>L>'
        OPERATION_HEADER_LENGTH = 10 # \x0, C op, L> key length, L> value length
        OPERATIONS = {
            :set => 1,
            :add => 2,
            :delete => 3,
            1 => :set,
            2 => :add,
            3 => :delete
        }

        attr_reader :key, :op, :value

        def initialize(key, op, value = nil)
            @key = key
            @op = op
            @value = value
        end

        def to_s
            key_data     = @key.to_s
            key_length   = key_data.length

            unless @value.nil?
                value_data   = @value.to_s
                value_length = value_data.length
            else
                value_data   = ''
                value_length = 0
            end

            header = [OPERATION_MAGIC, OPERATIONS[@op], key_length, value_length]
            header.pack(OPERATION_HEADER_FORMAT) + key_data + value_data
        end

        def self.from_file(file)
            header = file.read(OPERATION_HEADER_LENGTH)
            magic, op, key_length, value_length = header.unpack(OPERATION_HEADER_FORMAT)
            raise 'invalid operation header start byte' unless magic == OPERATION_MAGIC

            key_data = file.read(key_length)
            key = Key.from_s(key_data)

            if value_length > 0
                value_data = file.read(value_length)
                value = Value.from_s(value_data)
            else
                value = nil
            end

            Operation.new(key, OPERATIONS[op], value)
        end
    end

    class Transaction
        attr_reader :log, :operations
        TRANSACTION_MAGIC  = "\xFF"
        TRANSACTION_HEADER_FORMAT = 'aL>L>'
        TRANSACTION_HEADER_LENGTH = 9 # \xFF, L> length, L> count

        def initialize(log, operations = [])
            @log = log
            @operations = operations
        end

        def set(key, value)
            @operations << Operation.new(key, :set, value)
        end

        def add(key, value)
            @operations << Operation.new(key, :add, value)
        end

        def delete(key)
            @operations << Operation.new(key, :delete)
        end

        def commit
            @log.add(self)
        end

        def to_s
            entries = @operations.collect(&:to_s)
            length = entries.map(&:length).reduce(:+)
            header = [TRANSACTION_MAGIC, length, entries.count]
            header.pack(TRANSACTION_HEADER_FORMAT) + entries.join
        end

        def self.from_log(log)
            file = log.file
            header = file.read(TRANSACTION_HEADER_LENGTH)
            magic, length, count = header.unpack(TRANSACTION_HEADER_FORMAT)
            raise 'invalid transaction header start byte' unless magic == TRANSACTION_MAGIC

            start_position = file.pos
            operations = count.times.collect do
                Operation.from_file(file)
            end

            raise 'transaction is an invalid length' unless file.pos == start_position + length
            Transaction.new(log, operations)
        end
    end
end
