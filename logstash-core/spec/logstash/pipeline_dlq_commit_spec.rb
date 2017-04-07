# encoding: utf-8
require "tmpdir"
require "spec_helper"
require "logstash/codecs/plain"
require_relative "../support/mocks_classes"

java_import org.logstash.common.DeadLetterQueueFactory

class SingleGeneratorInput < LogStash::Inputs::Base
  config_name "singlegenerator"
  milestone 2

  def register
  end

  def run(queue)
    queue << Logstash::Event.new
  end

  def close
  end
end

class DLQCommittingFilter < LogStash::Filters::Base
  config_name "dlq_commit"
  milestone 2

  def register()
    @dlq_writer = LogStash::Util::DeadLetterQueueFactory::get("pipeline_id")
  end

  def filter(event)
    @dlq_writer.write(event, config_name, id, "my reason")
  end

  def threadsafe?() true; end

  def close() end
end

describe LogStash::Pipeline do
  let(:pipeline_settings_obj) { LogStash::SETTINGS }
  let(:pipeline_id) { "test" }
  let(:pipeline_settings) do
    {
      "pipeline.workers" => 2,
      "pipeline.id" => pipeline_id,
      "dead_letter_queue.enable" => true,
      "path.dead_letter_queue" => Dir.mktmpdir
    }
  end
  let(:metric) { LogStash::Instrument::Metric.new(LogStash::Instrument::Collector.new) }
  let(:test_config) {
    <<-eos
        input { singlegenerator {  } }

        filter { dlq_commit {} }

        output { dummyoutput { } }
    eos
  }

  subject { LogStash::Pipeline.new(test_config, pipeline_settings_obj, metric) }

  before(:each) do
    pipeline_settings.each {|k, v| pipeline_settings_obj.set(k, v) }
    allow(LogStash::Plugin).to receive(:lookup).with("input", "singlegenerator").and_return(SingleGeneratorInput)
    allow(LogStash::Plugin).to receive(:lookup).with("codec", "plain").and_return(LogStash::Codecs::Plain)
    allow(LogStash::Plugin).to receive(:lookup).with("filter", "dlq_commit").and_return(DLQCommittingFilter)
    allow(LogStash::Plugin).to receive(:lookup).with("output", "dummyoutput").and_return(::LogStash::Outputs::DummyOutput)
  end

  after(:each) do
    FileUtils.remove_entry pipeline_settings["path.dead_letter_queue"]
  end


  it "retrieves proper pipeline-level DLQ writer" do
    subject.run
    subject.close
    dlq_path = java.nio.file.Paths.get(pipeline_settings_obj.get("path.dead_letter_queue"), pipeline_id)
    dlq_reader = org.logstash.common.io.DeadLetterQueueReadManager.new(dlq_path)
    commit_count = 0
    (0..30).each do |i|
      entry = dlq_reader.pollEntry(40)
      if i < 30
        commit_count += 1
      else
        expect(i).to eq(30)
        expect(entry).to be_nil
      end
    end
    expect(commit_count).to eq(30)
  end
end
