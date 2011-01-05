repositories.remote << 'http://www.ibiblio.org/maven2'

GUAVA = 'com.google.guava:guava:jar:r07'

define "javautils" do
    project.version= "0.1.0"
    compile.with GUAVA
    package :jar

    task :threadingtest => [:compile, :test] do
      specs = (project.compile.dependencies + project.test.compile.dependencies).flatten
      cp = Buildr.artifacts(specs).each(&:invoke).map(&:name).join(File::PATH_SEPARATOR)
      cp = cp + ":" + path_to('target', 'classes') + ":" + path_to('target', 'test', 'classes')
      cmd = "java -cp #{cp} org.junit.runner.JUnitCore com.jaydonnell.javautils.MultiThreadedIT"
      system cmd
    end

    task :stresstest => [:compile, :test] do
      
    end
end