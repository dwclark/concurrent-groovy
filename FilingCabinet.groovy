import groovyx.gpars.activeobject.ActiveMethod;
import groovyx.gpars.activeobject.ActiveObject;
import groovyx.gpars.dataflow.DataflowVariable;
import java.util.concurrent.ThreadLocalRandom;
import static Pools.*

initialize();

final File ROOT = new File('/home/david/tmp/cabinets');
final List<String> CATEGORIES = [ 'lame', 'ok', 'awesome' ].asImmutable();

@ActiveObject(IO_GROUP_NAME)
class Cabinet {
    final int modulo;
    final File folder;
    final Map<String,RandomAccessFile> files = [:]

    Cabinet(File root, int modulo) {
        this.modulo = modulo;
        this.folder = new File(root, modulo as String);
        if(!folder.exists()) {
            folder.mkdir();
        }
    }

    private RandomAccessFile file(final String category) {
        RandomAccessFile ret = files[category];
        if(ret == null) {
            ret = new RandomAccessFile(new File(folder, category), 'rw');
            ret.seek(ret.length());
            files[category] = ret;
        }

        return ret;
    }
    
    @ActiveMethod
    DataflowVariable save(int id, String category, String info) {
        return saveIt(id, category, info);
    }

    boolean saveIt(int id, String category, String info) {
        RandomAccessFile raf = file(category);
        raf.writeInt(id);
        raf.writeUTF(info);
        return true;
    }
        
    @ActiveMethod
    DataflowVariable read(int id, String category) {
        return readIt(id, category);
    }

    private List<String> readIt(int id, String category) {
        RandomAccessFile raf = file(category);
        long current = raf.filePointer;
        List<String> lines = [];
        raf.seek(0);
        try {
            while(true) {
                int foundId = raf.readInt();
                String info = raf.readUTF();
                if(foundId == id) {
                    lines << info;
                }
            }
        }
        catch(EOFException eof) {
            //we are being lazy, ignore
        }
        finally {
            raf.seek(current);
        }

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

final int LEVEL = 20;
def cabinets = new Cabinets(ROOT, LEVEL);

def threads = (0..LEVEL).collect {
    Thread.start {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        100_000.times {
            int id = r.nextInt(1, 100);
            String cat = CATEGORIES[r.nextInt(3)];
            cabinets.save(id, cat, randomText(r.nextInt(10, 50))); }; }; };

threads.each { it.join(); }
println("************************************************************");
println("Reading id 57, modulo: ${57 % 20}, category: ${CATEGORIES[2]}");
println(cabinets.read(57, CATEGORIES[2]).val);
shutdown();
