import groovyx.gpars.dataflow.*;
import static Pools.*;
import java.util.concurrent.ThreadLocalRandom;

//We want to model an equation with lots of dependencies
//y = a*x2 + b*x * c
//x = m * r
//a = q * 17
//b = r / s
//c = 5 + s
//what are the things we need to know?: s, q, m, r

final DataflowBroadcast s = new DataflowBroadcast();
final DataflowBroadcast q = new DataflowBroadcast();
final DataflowBroadcast m = new DataflowBroadcast();
final DataflowBroadcast r = new DataflowBroadcast();

final DataflowReadChannel s2cRead = s.createReadChannel();
def c = { -> return 5 + s2cRead.val; }

final DataflowReadChannel r2bRead = r.createReadChannel();
final DataflowReadChannel s2bRead = s.createReadChannel();
def b = { -> return r2bRead.val / s2bRead.val; }

final DataflowReadChannel q2aRead = q.createReadChannel();
def a = { -> return q2aRead.val * 17; }

final DataflowReadChannel m2xRead = m.createReadChannel();
final DataflowReadChannel r2xRead = r.createReadChannel();
def x = { -> return m2xRead.val * r2xRead.val; }

def y = { ->
    def myX = x();
    return (a() * myX * myX) + (b() * myX) + c();
}

def rand = { max -> ThreadLocalRandom.current().nextLong(max); }

COMPUTE_GROUP.task { sleep(rand(1000L)); s << 5; }
COMPUTE_GROUP.task { sleep(rand(1500L)); q << 10; }
COMPUTE_GROUP.task { sleep(rand(700L)); m << 15; }
COMPUTE_GROUP.task { sleep(rand(100L)); r << 20; }

long nanos = timeIt {
    println("y is: ${y()}")
}

println("Total time to compute: ${nanos / 1_000_000} ms")

