import java.util.concurrent.ThreadLocalRandom;
import groovyx.gpars.dataflow.DataflowVariable;
import static Pools.*;

final DataflowVariable m = new DataflowVariable();
final DataflowVariable a = new DataflowVariable();

COMPUTE_GROUP.task { ->
    println("Computing mass");
    sleep(ThreadLocalRandom.current().nextLong(1000L))
    m << 10;
}

COMPUTE_GROUP.task { ->
    println("Computing acceleration");
    sleep(ThreadLocalRandom.current().nextLong(1000L));
    a << 15;
}

def F = m.val * a.val;

println "Force is ${F}"

//can also so this directly
F = (COMPUTE_GROUP.task { -> sleep(ThreadLocalRandom.current().nextLong(1000L)); return 10; }.val *
     COMPUTE_GROUP.task { -> sleep(ThreadLocalRandom.current().nextLong(1000L)); return 15; }.val);

println "Force is still ${F}"
