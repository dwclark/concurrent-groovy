import groovy.transform.CompileDynamic;
import groovy.transform.CompileStatic;
import groovyx.gpars.extra166y.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.*;
import java.util.stream.*;
import jsr166y.ForkJoinPool;
import static Pools.*;
import static groovyx.gpars.GParsPool.withExistingPool;

//These are the main tests. Any test that can be static is. This eliminates measuring
//groovy dynamic dispatch. However, GPars parallel collections cannot be compiled static
//because parallel collections are only installed at the meta class level at runtime.
@CompileStatic
class AllTests {
    
    static String doTiming(Closure c, String description) {
        double val;
        long nanos = timeIt { val = (double) c.call(); }
        return "Ran ${description} in ${nanos / 1_000_000} ms, result: ${val}";
    }
    
    static double groovyClosuresWithJava8Streams(long[] ary, int iterations) {
        double tmp;
        for(int i = 0; i < iterations; ++i) {
            tmp = (LongStream.of(ary)
                   .parallel()
                   .filter({ long l -> l % 2L == 0L; })
                   .mapToInt(Long.&bitCount)
                   .average()
                   .asDouble);
        }

        return tmp;
    }

    static double groovyStaticWithJava8Streams(long[] ary, int iterations) {
        double tmp;
        LongPredicate longPred = new LongPredicate() { boolean test(long lVal) { return lVal % 2L == 0L; } };
        LongToIntFunction long2Int = new LongToIntFunction() { int applyAsInt(long lVal) { return Long.bitCount(lVal); } };
        
        for(int i = 0; i < iterations; ++i) {
            tmp = (LongStream.of(ary)
                   .parallel()
                   .filter(longPred)
                   .mapToInt(long2Int)
                   .average()
                   .asDouble);
        }

        return tmp;
    }

    static double gparsJsr166y(long[] ary, int iterations, ForkJoinPool fjPool) {
        double tmp;
        Ops.LongPredicate longPred = new Ops.LongPredicate() { boolean op(long lng) { return (lng % 2) == 0L; } };
        Ops.LongOp longOp = new Ops.LongOp() {  long op(final long lng) { return (long) Long.bitCount(lng); } };
        
        for(int i = 0; i < iterations; ++i) {
            ParallelLongArrayWithFilter plawf = (ParallelLongArray.createFromCopy(ary, fjPool)
                                                 .withFilter(longPred)
                                                 .replaceWithMapping(longOp));
            ParallelLongArray.SummaryStatistics s = plawf.summary();
            tmp = plawf.sum() / s.size();
        }

        return tmp;
    }

    @CompileDynamic //has to be this way because parallel ops are added via dynamic meta class enhancements
    static double gparsParallelCollections(long[] ary, int iterations, ForkJoinPool fjPool) {
        double tmp;
        for(int i = 0; i < iterations; ++i) {
            withExistingPool(fjPool) {
                def intermediate = ary.findAllParallel({ long l -> l % 2L == 0L; }).collectParallel(Long.&bitCount);
                tmp = intermediate.sumParallel() / intermediate.size();
            }
        }

        return tmp;
    }

    static double singleThread(long[] ary, int iterations) {
        double tmp;
        for(int iter = 0; iter < iterations; ++iter) {
            long sum = 0L;
            long count = 0L;
            for(int i = 0; i < ary.length; ++i) {
                long val = ary[i];
                if(val % 2L == 0L) {
                    sum += Long.bitCount(val);
                    ++count;
                }
            }
            
            tmp = ((double) sum) / ((double) count);
        }

        return tmp;
    }
}


final long[] warmUp = ThreadLocalRandom.current().longs(100).toArray();
final int warmUpCount = 20_000;

final long[] longs = ThreadLocalRandom.current().longs(1_000_000).toArray();
final int longsCount = 20;

//do warmups
AllTests.doTiming({ -> AllTests.groovyClosuresWithJava8Streams(warmUp, warmUpCount); }, "Groovy 8 Closures With Java 8 Streams");
AllTests.doTiming({ -> AllTests.groovyStaticWithJava8Streams(warmUp, warmUpCount); }, "Groovy Static With Java 8 Streams");
AllTests.doTiming({ -> AllTests.gparsJsr166y(warmUp, warmUpCount, COMPUTE_POOL.forkJoinPool); }, "GPars JSR 166y Parallel Arrays");
AllTests.doTiming({ -> AllTests.gparsParallelCollections(warmUp, warmUpCount, COMPUTE_POOL.forkJoinPool); }, "GPars Parallel Collections");
AllTests.doTiming({ -> AllTests.singleThread(warmUp, warmUpCount); }, "Single Thread"); 

//now do the real thing
println(AllTests.doTiming({ -> AllTests.groovyClosuresWithJava8Streams(longs, longsCount); }, "Groovy 8 Closures With Java 8 Streams"));
println(AllTests.doTiming({ -> AllTests.groovyStaticWithJava8Streams(longs, longsCount); }, "Groovy Static With Java 8 Streams"));
println(AllTests.doTiming({ -> AllTests.gparsJsr166y(longs, longsCount, COMPUTE_POOL.forkJoinPool); }, "GPars JSR 166y Parallel Arrays"))
println(AllTests.doTiming({ -> AllTests.gparsParallelCollections(longs, longsCount, COMPUTE_POOL.forkJoinPool); }, "GPars Parallel Collections"));
println(AllTests.doTiming({ -> AllTests.singleThread(longs, longsCount); }, "Single Thread"));
