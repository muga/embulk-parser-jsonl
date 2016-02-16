require 'json'
require "embulk/parser/jsonl.rb"

module Embulk
  module Guess
    # $ embulk guess -g "jsonl" partial-config.yml

    class Jsonl < GuessPlugin
      Plugin.register_guess("jsonl", self)

      def guess(config, sample_buffer)
        return {} unless config.fetch("parser", {}).fetch("type", "jsonl") == "jsonl"

        rows = []

        json_parser = new_json_parser(sample_buffer)
        begin
          while true
            v = json_parser.next
            break unless v
            rows << JSON.parse(v.toJson)
          end
        rescue org.embulk.spi.json::JsonParseException
          # ignore
        end

        return {} if rows.size <= 3

        columns = Embulk::Guess::SchemaGuess.from_hash_records(rows)
        parser_guessed = {"type" => "jsonl"}
        parser_guessed["columns"] = columns

        return {"parser" => parser_guessed}
      end

      def new_json_parser(buffer)
        input_stream = java.io::ByteArrayInputStream.new(buffer.to_java_bytes)
        input_streams = com.google.common.collect::Lists::newArrayList(input_stream)

        buffer_allocator = org.embulk.spi.Exec::getBufferAllocator()
        iterator_provider = org.embulk.spi.util::InputStreamFileInput::IteratorProvider.new(input_streams)
        file_input = org.embulk.spi.util::InputStreamFileInput.new(buffer_allocator, iterator_provider)

        input = org.embulk.spi.util::FileInputInputStream.new(file_input)
        input.nextFile
        org.embulk.spi.json::JsonParser.new().open(input)
      end

    end
  end
end
