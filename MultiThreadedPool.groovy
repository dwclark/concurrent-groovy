import groovyx.gpars.dataflow.DataflowQueue;
import groovyx.gpars.dataflow.Promise;
import groovyx.gpars.dataflow.Select;
import groovyx.gpars.group.PGroup;
import static Pools.*;
import groovy.transform.CompileStatic;
import java.util.concurrent.atomic.AtomicInteger;

/*
  This file is not discussed in the demo/presentation.
  What it shows is just how easy it is to create a combination
  object pool/execution pool. Something like this can easily
  be used to execute all of your DB calls in a non-blocking
  and thread safe way. This could be a drop in replacement for
  something like Ratpack's ratpack.exec.Blocking.* calls.
 */
@CompileStatic
class FakeConnection {
    final int id;
    int counter = 0;

    public FakeConnection(final int id) { this.id = id; }
    public void reset() { counter = 0; }
    public String sql(String s) {
        ++counter;
        return "10,the description,10/10/2016"
    }
}

@CompileStatic
class ExecutionPool<T> {
    final DataflowQueue<T> theQueue = new DataflowQueue<>();
    final PGroup pGroup;
    
    public ExecutionPool(final List<T> toPool, final PGroup pGroup) {
        toPool.each { T t -> theQueue << t; }
        this.pGroup = pGroup;
    }

    public <R> Promise<R> call(final Closure<R> closure) {
        pGroup.with {
            T resource;
            return task {
                try {
                    resource = theQueue.val;
                    closure.call(resource);
                }
                finally {
                    if(resource != null) {
                        theQueue << resource;
                    }
                } }; };
    }
}
initialize();
final THREADS = 12;
final ITERATIONS = 100_000;
final COUNTER = new AtomicInteger();

def connections = (0..9).collect { new FakeConnection(it); };
def epool = new ExecutionPool<FakeConnection>(connections, COMPUTE_GROUP);
def executeSql = { FakeConnection fc -> fc.sql('select * from foo'); };
def threadCall = { -> epool.call(executeSql); };

def threads = (0..<THREADS).collect {
    Thread.start {
        List<Promise> promises = (0..<ITERATIONS).collect { epool.call(executeSql); };
        promises.each { Promise p -> p.get(); COUNTER.incrementAndGet(); }; }; };

threads.each { it.join(); }
assert(COUNTER.get() == (THREADS * ITERATIONS));
connections.each { FakeConnection fc -> println("#${fc.id} called ${fc.counter} times"); };
shutdown();
