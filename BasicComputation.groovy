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

//Notice that the broadcasts we set up are EXACTLY the same as the
//things we need to know, once these values are known computation can proceed.
final DataflowBroadcast s = new DataflowBroadcast();
final DataflowBroadcast q = new DataflowBroadcast();
final DataflowBroadcast m = new DataflowBroadcast();
final DataflowBroadcast r = new DataflowBroadcast();

//The main idea here is to set up read channels for each of the derived
//expressions. We don't share read channels so that each closure can
//read from the channel when it needs to and won't interfere with other read operations.
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

//notice how the groovy code for computing y is almost exaclty the
//same as the actual problem definition
def y = { ->
    def myX = x();
    return (a() * myX * myX) + (b() * myX) + c();
}

//Add in a measure of randomness to make sure that reading from the channels
//works no matter how long the pause times are. Threading bugs often show up
//when timings are different, we want to force ourselves and dataflow to be honest.
def rand = { max -> ThreadLocalRandom.current().nextLong(max); }
COMPUTE_GROUP.task { sleep(rand(1000L)); s << 5; }
COMPUTE_GROUP.task { sleep(rand(1500L)); q << 10; }
COMPUTE_GROUP.task { sleep(rand(700L)); m << 15; }
COMPUTE_GROUP.task { sleep(rand(100L)); r << 20; }

long nanos = timeIt {
    println("y is: ${y()}")
}

println("Total time to compute: ${nanos / 1_000_000} ms")

