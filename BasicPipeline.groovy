import static Pools.*;
import java.util.zip.GZIPOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import groovyx.gpars.dataflow.operator.PoisonPill;
import groovy.transform.CompileStatic;
import groovyx.gpars.dataflow.Promise;
import groovyx.gpars.dataflow.DataflowVariable;

initialize();

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

final int ITERATIONS = 1_000_000;
final LinkedBlockingQueue<Promise<?>> promiseQueue = new LinkedBlockingQueue<>();
final STOP = PoisonPill.instance
final DataflowVariable DF_STOP = new DataflowVariable();
DF_STOP.bind(STOP);

for(int i = 0; i < ITERATIONS; ++i) {
    promiseQueue.put(COMPUTE_GROUP.task(Pools.&randomText.curry(60))
                     .then({ String s -> new StringBuilder(s).reverse().toString(); })
                     .then({ String s -> '  ' + s + '  '; })
                     .then(Operations.&rot13));
}

promiseQueue.put(DF_STOP);

assert(ITERATIONS == Operations.drainUntil(promiseQueue, STOP));

shutdown();

