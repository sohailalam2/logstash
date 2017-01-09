
rule ".rb" => ".treetop" do |task, args|
  require "treetop"
  compiler = Treetop::Compiler::GrammarCompiler.new
  compiler.compile(task.source, task.name)
  puts "Compiling #{task.source}"
end

namespace "compile" do
  desc "Compile the config grammar"

  task "grammar" => "lib/logstash/config/grammar.rb"

  task "logstash-java" do
    puts("Building logstash java components")
    system("./gradlew", "jar")
  end

  desc "Build everything"
  task "all" => ["grammar", "logstash-java"]
end
