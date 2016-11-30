import groovyx.gpars.activeobject.ActiveMethod;
import groovyx.gpars.activeobject.ActiveObject;
import groovyx.gpars.dataflow.DataflowVariable;
import java.util.concurrent.ThreadLocalRandom;
import static Pools.*;

initialize();

//define the categories. Think of them as similar to logging levels.
//In a database transaction log system these might be session ids.
final List<String> CATEGORIES = [ 'lame', 'ok', 'awesome' ].asImmutable();

/*
  This is our basic cabinet. The idea here is to quickly write out data
  based on the id and category we are saving. Each cabinet manages a folder
  which in which it creates one folder for each category.

  Why is this useful? Relational databases do something very
  similar in writing transaction logs. The basic idea is to partition
  the transaction logs in a logical way. The database would then
  save the timestamp along with transaction details. The key to DB transaction
  logs is that they have to be fast and safe. Every transaction has
  to end up on disk. On database restart the database can then
  very quickly scan the transaction logs and validate the transactions
  have committed. Databases will also prune their logs (based on configuration)
  once they know they can verify that the contents of the transaction have
  been committed to the approprite tables.

  Our implementation is much less sophisticated, but actors make the
  thread safety of the files easy.
 */
@ActiveObject(IO_GROUP_NAME)
class Cabinet {
    final int modulo;
    final File folder;
    final Map<String,RandomAccessFile> files = [:]

    //set up the root folder for this cabinet
    Cabinet(File root, int modulo) {
        this.modulo = modulo;
        this.folder = new File(root, modulo as String);
        if(!folder.exists()) {
            folder.mkdir();
        }
    }

    //return the appropriate file for writing/reading
    //create the file if it doesn't exist.
    //We use RandomAccessFile for speed in writing and seeking
    private RandomAccessFile file(final String category) {
        RandomAccessFile ret = files[category];
        if(ret == null) {
            ret = new RandomAccessFile(new File(folder, category), 'rw');
            ret.seek(ret.length());
            files[category] = ret;
        }

        return ret;
    }

    //This is the main actor method for saving a new piece of info
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

    //This is the main actor method getting information out of the cabinets
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

//Top level class for organizing all cabinets. The Cabinets
//class is responsible for managing the top level folder and for finding
//the appropriate cabinet for saving the information
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

//Simple test to make sure we can pump data in safely and quickly
//We basically just generate random ids and texts and pump
//them through the cabinets as quickly as possible
//On Linux I was able to be 20 MB/s sustained throughput on writes.
final int LEVEL = 20;
def cabinets = new Cabinets(CABINETS_DIRECTORY, LEVEL);

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

//make sure the read operation works
println(cabinets.read(57, CATEGORIES[2]).val);
shutdown();
