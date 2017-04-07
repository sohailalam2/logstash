# encoding: utf-8

require 'logstash/environment'

module LogStash; module Util
  class DeadLetterQueueWriter

    def initialize(inner_writer)
      @inner_writer = inner_writer
    end

    def write(logstash_event, plugin_type, plugin_id, reason)
      if @inner_writer && @inner_writer.is_open
        @inner_writer.writeEntry(logstash_event.to_java, plugin_type, plugin_id, reason)
      end
    end
  end

  class DeadLetterQueueFactory
    java_import org.logstash.common.DeadLetterQueueFactory

    def self.get(pipeline_id)
      if LogStash::SETTINGS.get("dead_letter_queue.enable")
        return DeadLetterQueueWriter.new(
            DeadLetterQueueFactory.getWriter(pipeline_id, LogStash::SETTINGS.get("path.dead_letter_queue")))
      else
        return DeadLetterQueueWriter.new(nil)
      end
    end

    def self.close(pipeline_id)
      DeadLetterQueueFactory.close(pipeline_id)
    end
  end
end end
