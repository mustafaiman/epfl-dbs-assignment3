import java.util.*;

/**
 * implement a (main-memory) data store with MVTO.
 * objects are <int, int> key-value pairs.
 * if an operation is to be refused by the MVTO protocol,
 * undo its xact (what work does this take?) and throw an exception.
 * garbage collection of versions is not required.
 * Throw exceptions when necessary, such as when we try to execute an operation in
 * a transaction that is not running; when we insert an object with an existing
 * key; when we try to read or write a nonexisting key, etc.
 * Keep the interface, we want to test automatically!
 **/


public class MVTO {
  /* TODO -- your versioned key-value store data structure */

    private static HashMap<Integer, Transaction> activeTransactionsByTimestamp = new HashMap<>();

    private static int max_xact = 0;

    // returns transaction id == logical start timestamp
    public static int begin_transaction() {
        max_xact++;
        Transaction txn = new Transaction(max_xact);
        activeTransactionsByTimestamp.put(max_xact, txn);
        return max_xact;
    }

    // create and initialize new object in transaction xact
    public static void insert(int xact, int key, int value) throws Exception {

    }

    // return value of object key in transaction xact
    public static int read(int xact, int key) throws Exception {
    /* TODO */
        return 0; //TODO: RETURN THE CORRECT VALUE//
    }

    // write value of existing object identified by key in transaction xact
    public static void write(int xact, int key, int value) throws Exception {
    /* TODO */
    }

    public static void commit(int xact) throws Exception {
    /* TODO */
    }

    public static void rollback(int xact) throws Exception {
    /* TODO */
    }
}


class Value {
    final int key;
    LinkedList<ValueVersion> versions;

    Value(int key, int initialValue, int timestamp) {
        this.key = key;
        this.versions = new LinkedList<>();
        ValueVersion initialVersion = new ValueVersion(timestamp, 0, initialValue);
        this.versions.add(initialVersion);
    }

    void createNewVersion(int content, long rts, long wts) {
    }

    ValueVersion getBeforeWriteTimestamp(long timestamp) {
        throw new RuntimeException("Not implemented");
    }


}

class ValueVersion {
    final int rts;
    final int wts;
    private int content;

    ValueVersion(int rts, int wts, int content) {
        this.rts = rts;
        this.wts = wts;
        this.content = content;
    }

    void setContent(int content) {
        this.content = content;
    }

    int getContent() {
        return this.content;
    }
}

class Transaction {
    final int timestamp;
    LinkedList<Integer> dependenciesToTimestamps = new LinkedList<>();

    Transaction(int timestamp) {
        this.timestamp = timestamp;
    }

}

