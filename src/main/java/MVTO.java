import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

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

    private static HashMap<Integer, Transaction> activeTransactionsById = new HashMap<>();

    private static HashMap<Integer, Value> kvStore = new HashMap<>();

    private static int max_xact = 0;

    // returns transaction id == logical start timestamp
    public static int begin_transaction() {
        max_xact++;
        Transaction txn = new Transaction(max_xact);
        activeTransactionsById.put(txn.getTimestamp(), txn);
        return max_xact;
    }

    // create and initialize new object in transaction xact
    public static void insert(int xact, int key, int value) throws Exception {
        Value valObj = kvStore.get(key);
        if (valObj == null) {
            Transaction activeTxn = activeTransactionsById.get(xact);
            Value newVal = new Value(key, value, activeTxn.getTimestamp());
            kvStore.put(key, newVal);
            activeTxn.addToLog(key);
        } else {
            System.out.println("Key: " + key + " already exists.");
            rollback(xact);
        }
    }

    // return value of object key in transaction xact
    public static int read(int xact, int key) throws Exception {
        Value val = kvStore.get(key);
        Transaction txn = activeTransactionsById.get(xact);
        ValueVersion versioned = val.getBeforeWriteTimestamp(txn.getTimestamp());
        if (versioned.getRts() < txn.getTimestamp()) {
            versioned.setRts(txn.getTimestamp());
        }
        versioned.addDependant(xact);
        txn.waitOne();
        return versioned.getContent();
    }

    // write value of existing object identified by key in transaction xact
    public static void write(int xact, int key, int value) throws Exception {
        Value val = kvStore.get(key);
        Transaction txn = activeTransactionsById.get(xact);
        ValueVersion versioned = val.getBeforeWriteTimestamp(txn.getTimestamp());
        if (txn.getTimestamp() < versioned.getRts()) {
            rollback(xact);
        } else if (txn.getTimestamp() >= versioned.getRts() && txn.getTimestamp() == versioned.getWts()) {
            val.createNewVersion(value, versioned.getRts(), txn.getTimestamp());
            //TODO remove old one
        } else if (txn.getTimestamp() >= versioned.getRts() && versioned.getWts() < txn.getTimestamp()) {
            val.createNewVersion(value, txn.getTimestamp(), txn.getTimestamp());
        }
    }

    public static void commit(int xact) throws Exception {
        Transaction txn = activeTransactionsById.get(xact);
        if (!txn.isWaiting()) {
            List<Integer> dependants = txn.getLog();
            for (Integer possibleWaiter: dependants) {
                Transaction waiter = activeTransactionsById.get(possibleWaiter);
                if (waiter != null) {
                    waiter.waitLess();
                    if (!waiter.isWaiting()) {
                        commit(waiter.getTimestamp());
                    }
                }
            }
            activeTransactionsById.remove(xact);
        }
    }

    public static void rollback(int xact) throws Exception {
        AbstractQueue<Integer> rollbackQueue = new LinkedBlockingDeque<>();
        rollbackQueue.add(xact);
        while (!rollbackQueue.isEmpty()) {
            int txnId = rollbackQueue.remove();
            Transaction txn = activeTransactionsById.remove(txnId);
            LinkedList<Integer> ops = txn.getLog();
            for (Integer key: ops) {
                ValueVersion removedVersion = kvStore.get(key).removeVersion(txn.getTimestamp());
                for (Integer dependantTxnId: removedVersion.getDependants()) {
                    rollbackQueue.add(dependantTxnId);
                }
            }
        }
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

    void createNewVersion(int content, int rts, int wts) {
        ValueVersion versioned = new ValueVersion(rts, wts, content);
        Iterator<ValueVersion> it = versions.descendingIterator();
        ValueVersion addAfter = null;
        int i = versions.size();
        while (it.hasNext()) {
            addAfter = it.next();
            if ( addAfter.getWts() < versioned.getWts() ) {
                break;
            }
        }
        if (i == versions.size()) {
            versions.addLast(versioned);
        } else if (i == 0) {
            versions.addFirst(versioned);
        } else {
            versions.add(i, versioned);
        }
    }

    ValueVersion getBeforeWriteTimestamp(int timestamp) {
        ValueVersion last = null;
        for (ValueVersion vers: versions) {
            if (vers.getWts() <= timestamp) {
                last = vers;
            } else {
                break;
            }
        }
        return last;
    }

    public ValueVersion removeVersion(int wts) {
        return null;
    }

}

class ValueVersion {
    private int rts;
    private int wts;
    private int content;
    private LinkedList<Integer> dependants = new LinkedList<>();

    ValueVersion(int rts, int wts, int content) {
        this.rts = rts;
        this.wts = wts;
        this.content = content;
    }

    public int getRts() {
        return this.rts;
    }

    public void setRts(int rts) {
        this.rts = rts;
    }

    public int getWts() {
        return this.wts;
    }

    void setContent(int content) {
        this.content = content;
    }

    int getContent() {
        return this.content;
    }

    public void addDependant(int txnId) {
        dependants.add(txnId);
    }

    public List<Integer> getDependants() {
        return this.dependants;
    }
}

class Transaction {
    final private int timestamp;
    private LinkedList<Integer> addedByThis = new LinkedList<>();
    private int waitingFor;

    public Transaction(int timestamp) {
        this.timestamp = timestamp;
        this.waitingFor = 0;
    }

    public int getTimestamp() {
        return this.timestamp;
    }

    public void addToLog(Integer key) {
        addedByThis.add(key);
    }

    public LinkedList<Integer> getLog() {
        return this.addedByThis;
    }

    public boolean isWaiting() {
        return waitingFor != 0;
    }

    public void waitOne() {
        waitingFor++;
    }

    public void waitLess() {
        waitingFor--;
    }

}

