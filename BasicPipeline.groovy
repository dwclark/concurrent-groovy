import static Pools.*;
import java.util.zip.GZIPOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import groovyx.gpars.dataflow.operator.PoisonPill;
import groovy.transform.CompileStatic;
import groovyx.gpars.dataflow.Promise;
import groovyx.gpars.dataflow.DataflowVariable;

initialize();

/*
  These are just utility methods for use later. Note how
  we @CompileStatic this stuff for performance. Encryption code
  is always performance sensitive, but application code generally isn't.
  Groovy gives us the best of both worlds, fast and easy to use.
 */
@CompileStatic
class Operations {
    private static final char a = 'a'; private static final char m = 'm';
    private static final char A = 'A'; private static final char M = 'M';
    private static final char n = 'n'; private static final char z = 'z';
    private static final char N = 'N'; private static final char Z = 'Z';

    static String rot13(final String str) {
        StringBuilder sb = new StringBuilder(str.length());
        for(int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if(c >= a && c <= m) {
                sb.append(c + 13);
            }
            else if(c >= A && c <= M) {
                sb.append(c + 13);
            }
            else if(c >= n && c <= z) {
                sb.append(c - 13);
            }
            else if(c >= N && c <= Z) {
                sb.append(c - 13);
            }
            else {
                sb.append(c);
            }
        }

        return sb.toString();
    }
    
    static <T> int drainUntil(LinkedBlockingQueue<Promise<?>> promises, Object stop) {
        int computed = 0;
        while(true) {
            Object o = promises.take().get();
            if(!o.is(stop)) {
                ++computed;
            }
            else {
                break;
            }
        }

        return computed;
    }
}

//Set up variables for work and say how many times we will process text values
final int ITERATIONS = 1_000_000;
final LinkedBlockingQueue<Promise<?>> promiseQueue = new LinkedBlockingQueue<>();
final STOP = PoisonPill.instance
final DataflowVariable DF_STOP = new DataflowVariable();
DF_STOP.bind(STOP);

/*
  Basic task:
  1) Generate some random text
  2) Reverse the text
  3) pad the text
  4) "Encrypt" the text
  5) Save the value for the downstream consumer
 */
for(int i = 0; i < ITERATIONS; ++i) {
    promiseQueue.put(COMPUTE_GROUP.task(Pools.&randomText.curry(60))
                     .then({ String s -> new StringBuilder(s).reverse().toString(); })
                     .then({ String s -> '  ' + s + '  '; })
                     .then(Operations.&rot13));
}

/*
  Once we have done our work we enqueue a "poison pill" to signal
  to the queue and consumers that there is nothing left to do and
  it is safe to shut down.
 */
promiseQueue.put(DF_STOP);

//make sure we stop correctly
assert(ITERATIONS == Operations.drainUntil(promiseQueue, STOP));

shutdown();

