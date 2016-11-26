import groovy.transform.CompileDynamic;
import groovy.transform.CompileStatic;
import groovyx.gpars.extra166y.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.*;
import java.util.stream.*;
import jsr166y.ForkJoinPool;
import static Pools.*;
import static groovyx.gpars.GParsPool.withExistingPool;

//Some statically compile helper classes for Java 8 Streams
//and GPars JSR 166y parallel arrays
@CompileStatic class IsEven implements LongPredicate, Ops.LongPredicate {
    final static IsEven instance = new IsEven();
    boolean test(final long lng) {
        return (lng % 2L) == 0L
    }

    boolean op(final long lng) {
        return (lng % 2) == 0L;
    }
}

@CompileStatic class BitCount implements LongToIntFunction, Ops.LongOp {
    final static BitCount instance = new BitCount();
    int applyAsInt(final long lng) {
        return Long.bitCount(lng);
    }

    long op(final long lng) {
        return (long) Long.bitCount(lng);
    }
}

//These are the main tests. Any test that can be static is eliminate measuring
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
                   .mapToInt({ long l -> Long.bitCount(l); })
                   .average()
                   .asDouble);
        }

        return tmp;
    }

    static double groovyStaticWithJava8Streams(long[] ary, int iterations) {
        double tmp;
        for(int i = 0; i < iterations; ++i) {
            tmp = (LongStream.of(ary)
                   .parallel()
                   .filter(IsEven.instance)
                   .mapToInt(BitCount.instance)
                   .average()
                   .asDouble);
        }

        return tmp;
    }

    static double gparsJsr166y(long[] ary, int iterations, ForkJoinPool fjPool) {
        double tmp;
        for(int i = 0; i < iterations; ++i) {
            ParallelLongArrayWithFilter plawf = (ParallelLongArray.createFromCopy(ary, fjPool)
                                                 .withFilter(IsEven.instance)
                                                 .replaceWithMapping(BitCount.instance));
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
                def intermediate = ary.findAllParallel(IsEven.instance.&test).collectParallel(BitCount.instance.&applyAsInt);
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

final long[] longs = ThreadLocalRandom.current().longs(1_000_000).toArray();
final long[] warmUp = ThreadLocalRandom.current().longs(100).toArray();

//do warmups
AllTests.doTiming({ -> AllTests.groovyClosuresWithJava8Streams(warmUp, 20_000); }, "Groovy 8 Closures With Java 8 Streams");
AllTests.doTiming({ -> AllTests.groovyStaticWithJava8Streams(warmUp, 20_000); }, "Groovy Static With Java 8 Streams");
AllTests.doTiming({ -> AllTests.gparsJsr166y(warmUp, 20_000, COMPUTE_POOL.forkJoinPool); }, "GPars JSR 166y Parallel Arrays");
AllTests.doTiming({ -> AllTests.gparsParallelCollections(warmUp, 20_000, COMPUTE_POOL.forkJoinPool); }, "GPars Parallel Collections");
AllTests.doTiming({ -> AllTests.singleThread(warmUp, 20_000); }, "Single Thread"); 

//now do the real thing
println(AllTests.doTiming({ -> AllTests.groovyClosuresWithJava8Streams(longs, 20); }, "Groovy 8 Closures With Java 8 Streams"));
println(AllTests.doTiming({ -> AllTests.groovyStaticWithJava8Streams(longs, 20); }, "Groovy Static With Java 8 Streams"));
println(AllTests.doTiming({ -> AllTests.gparsJsr166y(longs, 20, COMPUTE_POOL.forkJoinPool); }, "GPars JSR 166y Parallel Arrays"))
println(AllTests.doTiming({ -> AllTests.gparsParallelCollections(longs, 20, COMPUTE_POOL.forkJoinPool); }, "GPars Parallel Collections"));
println(AllTests.doTiming({ -> AllTests.singleThread(longs, 20); }, "Single Thread"));
