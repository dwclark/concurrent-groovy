import groovyx.gpars.activeobject.ActiveObjectRegistry;
import groovyx.gpars.group.DefaultPGroup;
import groovyx.gpars.scheduler.DefaultPool;
import groovyx.gpars.scheduler.FJPool;
import java.util.concurrent.*;
import groovy.transform.CompileStatic;

/*
  Grab bag utility methods for demos.
 */
class Pools {

    /*
      Define the pools used during demos. It is important to execute tasks on thread pools
      for performance and resource utilization management.
     */
    static final int CORES = Runtime.runtime.availableProcessors();
    static final int IO_MAX = CORES * 4;
    static final long IO_KEEP_ALIVE = 1L;
    static final TimeUnit IO_KEEP_ALIVE_UNITS = TimeUnit.MINUTES;
    private static final BlockingQueue<Runnable> JAVA_IO_QUEUE = new LinkedBlockingQueue(IO_MAX * 2);
    
    static final ThreadPoolExecutor JAVA_IO_POOL = new ThreadPoolExecutor(CORES, IO_MAX, IO_KEEP_ALIVE,
                                                                          IO_KEEP_ALIVE_UNITS, JAVA_IO_QUEUE);

    static final DefaultPool GROOVY_IO_POOL = new DefaultPool(JAVA_IO_POOL);
    static final String IO_GROUP_NAME = 'theActorGroup';
    static final DefaultPGroup IO_GROUP = new DefaultPGroup(GROOVY_IO_POOL);

    static final FJPool COMPUTE_POOL = new FJPool(CORES+1);
    static final DefaultPGroup COMPUTE_GROUP = new DefaultPGroup(COMPUTE_POOL);

    /*
      Methods to initialize and shutdown the pools. It is important to shutdown pools cleanly
      to make sure all in-flight tasks are executed and to actually terminate the program.
      Any pool running non-daemon threads will block the JVM from exiting.
     */
    static void initialize() {
        ActiveObjectRegistry.instance.register(IO_GROUP_NAME, IO_GROUP);
    }

    static void shutdown() {
        JAVA_IO_POOL.shutdown();
        GROOVY_IO_POOL.shutdown();
        COMPUTE_POOL.shutdown();
    }

    /*
      Utility methods shared/used by groovy scripts.
     */
    static final File WORKING_DIRECTORY = populateWorking();
    static final File COUNTS_DIRECTORY = populateCounts();
    static final File CABINETS_DIRECTORY = populateCabinets();

    static final String ALPHABET = (('A'..'Z')+('a'..'z')).join();
    
    static String randomText(int length) {
        StringBuilder sb = new StringBuilder(length);
        for(int i = 0; i < length; ++i) {
            sb.append(ALPHABET[ThreadLocalRandom.current().nextInt(ALPHABET.length())]);
        }
        
        return sb.toString();
    }
    
    static File populateWorking() {
        File tmp = new File("tmp");
        if(!tmp.exists()) tmp.mkdir();
        return tmp;
    }

    static File populateCounts() {
        File counts = new File(WORKING_DIRECTORY, 'counts');
        if(!counts.exists()) counts.mkdir();
        return counts;
    }

    static File populateCabinets() {
        File cabinets = new File(WORKING_DIRECTORY, 'cabinets');
        if(!cabinets.exists()) cabinets.mkdir();
        return cabinets;
    }

    static List<Thread> threadsTimes(final int threads, final int times, final Closure c) {
        return (0..<threads).collect { num ->
            Thread.start {
                for(int i = 0; i < times; ++i) {
                    c.call();
                } }; };
    }

    static void waitAll(List<Thread> threads) {
        threads.each { t -> t.join(); }
    }

    @CompileStatic
    static long timeIt(Closure c) {
        long begin = System.nanoTime();
        c.call();
        long end = System.nanoTime();
        return end - begin;
    }
}
