module Kafka
  class Preserver
    # Constants
    Undeliverable = 1
    Unassignable = 2

    def initialize(log_path, logger:)
      @file = File.open(log_path, 'a')
      @file.sync = true
      @logger = logger

      @starting_pos = 0
      @mutex = Mutex.new
    end

    #
    # Dump MessageBuffer to file
    #
    # @param [Kafka::MessageBuffer] buffer
    #
    # @return [void]
    #
    def dump(buffer)
      buffer.each do |topic, _, messages|
        messages.each do |message|
          line = serialize_record(topic, message)
          write(Kafka::Preserver::Undeliverable, line)
        end
      end
    end

    def dump_pending_queue(queue)
      queue.each do |message|
        line = serialize_pending_message(message)
        write(Kafka::Preserver::Unassignable, line)
      end
    end

    #
    # TODO
    # Read the first `max_count` lines
    #
    # @param [Integer] max_count:
    #
    # @return [Array]
    #
    def load(max_count: 100)
      # data = []

      # @mutex.synchronize do
      #   f = File.open(log_path, 'r')

      #   lines_count = 0
      #   f.seek(@starting_pos)
      #   f.each_line do |line|
      #     break if lines_count >= max_count

      #     data << deserialize(line)

      #     # write whole line with ' '
      #     f.seek(-line.length, IO::SEEK_CUR)
      #     f.write(' ' * (line.length - 1))
      #     f.write("\n")

      #     # record starting pos
      #     @starting_pos = @starting_pos + line.length
      #     # increase line count
      #     lines_count++
      #   end

      #   f.close
      #   return data
      # end
    end

    private
    #
    # 将一行数据写入 log
    #
    # @param [Integer] reason
    # @param [String] line
    #
    # @return [void]
    #
    def write(reason, line)
      @mutex.synchronize { @file.puts "#{Time.now.to_i},v1,#{reason},#{line}" }
    end

    #
    # TODO
    # 将一条 ProtocolRecord 转为可以写入 log 文件
    #
    # @param [String] topic
    # @param [Kafka::Protocol::Record] record
    #
    # @return [String]
    #
    def serialize_record(topic, record)
      MultiJson.dump({
        value: record.value,
        key: record.key,
        headers: record.headers,
        topic: topic,
        create_time: record.create_time.to_i
      })
    end

    def serialize_pending_message(message)
      MultiJson.dump({
        value: message.value,
        key: message.key,
        headers: message.headers,
        topic: message.topic,
        create_time: message.create_time.to_i
      })
    end

    #
    # TODO
    # 反序列化 - 从 json lines 反序列化出来 buffer
    #
    # @param [String] line
    #
    # @return [Hash]
    #
    def deserialize(line)
      # MultiJson.load(line)
    end
  end
end
