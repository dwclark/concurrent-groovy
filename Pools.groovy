import java.util.concurrent.*;
import groovyx.gpars.scheduler.DefaultPool;
import groovyx.gpars.group.DefaultPGroup;
import groovyx.gpars.activeobject.ActiveObjectRegistry;

class Pools {

    static final int CORES = Runtime.runtime.availableProcessors();
    static final int IO_MAX = CORES * 4;
    static final long IO_KEEP_ALIVE = 1L;
    static final TimeUnit IO_KEEP_ALIVE_UNITS = TimeUnit.MINUTES;
    private static final BlockingQueue<Runnable> IO_QUEUE = new LinkedBlockingQueue(IO_MAX * 2);
    
    static final ThreadPoolExecutor IO_POOL = new ThreadPoolExecutor(CORES, IO_MAX, IO_KEEP_ALIVE,
                                                                     IO_KEEP_ALIVE_UNITS, IO_QUEUE);

    static final DefaultPool ACTOR_IO_POOL = new DefaultPool(IO_POOL);
    static final String ACTOR_GROUP_NAME = 'theActorGroup';
    static final DefaultPGroup ACTOR_IO_GROUP = new DefaultPGroup(ACTOR_IO_POOL);

    static final String ALPHABET = (('A'..'N')+('P'..'Z')+('a'..'k')+('m'..'z')+('2'..'9')).join();
    
    static String randomText(int length) {
        return ThreadLocalRandom.current().with {
            (1..length).collect { ALPHABET[nextInt(ALPHABET.length())] }.join()
        }
    }
    
    static void initialize() {
        ActiveObjectRegistry.instance.register(ACTOR_GROUP_NAME, ACTOR_IO_GROUP);
    }

    static void shutdown() {
        IO_POOL.shutdown();
        ACTOR_IO_POOL.shutdown();
    }
}
