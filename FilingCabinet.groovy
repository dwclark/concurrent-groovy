import static Pools.*
import groovyx.gpars.activeobject.ActiveObject;
import groovyx.gpars.activeobject.ActiveMethod;
import groovyx.gpars.dataflow.DataflowVariable;
import java.util.concurrent.ThreadLocalRandom;

initialize();

final File ROOT = new File('/home/david/tmp/cabinets');
final List<String> CATEGORIES = [ 'lame', 'ok', 'awesome' ].asImmutable();

@ActiveObject(ACTOR_GROUP_NAME)
class Cabinet {
    final int modulo;
    final File folder;
    final Map<String,File> files = [:]

    Cabinet(File root, int modulo) {
        this.modulo = modulo;
        this.folder = new File(root, modulo as String);
        if(!folder.exists()) {
            folder.mkdir();
        }
    }

    private File file(final String category) {
        File ret = files[category];
        if(ret == null) {
            ret = new File(folder, category);
            files[category] = ret;
        }

        return ret;
    }
    
    @ActiveMethod
    DataflowVariable save(int id, String category, String info) {
        return saveIt(id, category, info);
    }

    boolean saveIt(int id, String category, String info) {
        file(category).withWriterAppend { writer ->
            writer.write(String.format("%d %s%n", id, info)); }
        return true;
    }
        
    @ActiveMethod
    DataflowVariable read(int id, String category) {
        return readIt(id, category);
    }

    private List<String> readIt(int id, String category) {
        List<String> lines = [];
        file(category).withReader { reader ->
            reader.eachLine { line ->
                int index = line.indexOf(' ');
                int num = line.substring(0, index) as int;
                if(num == id) {
                    lines << line.substring(index + 1); }; }; };
        return lines;
    }
}

class Cabinets {
    final int numberCabinets;
    final File root;
    final List<Cabinet> all;
    
    Cabinets(final File root, final int numberCabinets) {
        this.root = root;
        this.numberCabinets = numberCabinets;
        this.all = (0..<numberCabinets).collect { n -> new Cabinet(root, n); }
    }

    Cabinet find(final int id) {
        return all[id % numberCabinets];
    }

    DataflowVariable save(int id, String category, String info) {
        return find(id % numberCabinets).save(id, category, info);
    }

    DataflowVariable read(int id, String category) {
        return find(id % numberCabinets).read(id, category);
    }
}

def cabinets = new Cabinets(ROOT, 8);

def threads = (0..20).collect {
    Thread.start {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        100_000.times {
            int id = r.nextInt(1, 100);
            String cat = CATEGORIES[r.nextInt(3)];
            cabinets.save(id, cat, randomText(r.nextInt(10, 50))); }; }; };

threads.each { it.join(); }
println("************************************************************");
println("Reading id 57, modulo: ${57 % 8}, category: ${CATEGORIES[2]}");
println(cabinets.read(57, CATEGORIES[2]).val);
shutdown();
