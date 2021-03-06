repositories.remote << 'http://www.ibiblio.org/maven2'

GUAVA = 'com.google.guava:guava:jar:r07'
JCIP = 'net.jcip:jcip-annotations:jar:1.0'

define "javautils" do
    project.version= "0.2.3"
    compile.with GUAVA
    compile.with JCIP
    package :jar

    task :threadingtest => [:compile, :test] do
      specs = (project.compile.dependencies + project.test.compile.dependencies).flatten
      cp = Buildr.artifacts(specs).each(&:invoke).map(&:name).join(File::PATH_SEPARATOR)
      cp = cp + ":" + path_to('target', 'classes') + ":" + path_to('target', 'test', 'classes')
      cmd = "java -cp #{cp} org.junit.runner.JUnitCore com.jaydonnell.javautils.MultiThreadedIT"
      system cmd
    end

    task :stresstest => [:package] do
      require 'target/javautils-0.2.3.jar'

      specs = (project.compile.dependencies + project.test.compile.dependencies).flatten
      cp = Buildr.artifacts(specs).each(&:invoke).map(&:name).join(File::PATH_SEPARATOR)
      cp.split(":").select { |jar| jar =~ /jar/ }.each { |jar| require jar }
      
      aq = Java::ComJaydonnellJavautils::AlternatingMultiqueue.new(100000)
      enqueued = Java::JavaUtilConcurrent::LinkedBlockingQueue.new
      dequeued = Java::JavaUtilConcurrent::LinkedBlockingQueue.new

      enqueue_pool = Java::JavaUtilConcurrent::Executors.newFixedThreadPool(10)
      dequeue_pool = Java::JavaUtilConcurrent::Executors.newFixedThreadPool(1)

      10.times do
        enqueue_pool.execute(Enqueuer.new(aq, enqueued))
      end
      dequeue_pool.execute(Dequeuer.new(aq, dequeued))

      enqueue_pool.shutdown
      dequeue_pool.shutdown

      dequeue_pool.awaitTermination(100, Java::JavaUtilConcurrent::TimeUnit.valueOf("SECONDS"))
      
      enqueued.each do |v|
        #print v + " "
        raise "value #{v} not in dequeued" unless dequeued.remove(v)
      end
      puts "Stress test complete."

      raise "dequeued count does not matche enqueued count" unless dequeued.isEmpty
    end
end



class Enqueuer
  include java.lang.Runnable
  
  def initialize(queue, enqueued)
    @queue = queue
    @enqueued = enqueued
  end

  def run
    1000.times do
      v = rand(10000000).to_s
      @queue.put(rand(10).to_s, v)
      @enqueued.offer(v)
    end
  end
end

class Dequeuer
  include java.lang.Runnable

  def initialize(queue, dequeued)
    @queue = queue
    @dequeued = dequeued
  end

  def run
    10000.times do
      v = @queue.poll()
      @dequeued.offer(v)
    end
  end
end
