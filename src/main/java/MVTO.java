import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

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

    private static HashMap<Integer, Transaction> activeTransactionsById = new HashMap<>();

    private static HashMap<Integer, Value> kvStore = new HashMap<>();

    private static int max_xact = 0;

    private static Logger logger = Logger.getLogger(MVTO.class.getCanonicalName());

    // returns transaction id == logical start timestamp
    public static int begin_transaction() {
        max_xact++;
        Transaction txn = new Transaction(max_xact);
        activeTransactionsById.put(max_xact, txn);
        logger.info("Begin transaction " + max_xact);
        return max_xact;
    }

    // create and initialize new object in transaction xact
    public static void insert(int xact, int key, int value) throws Exception {
        logger.info("Insert issued for " + xact + " => {" + key + "," + value + "}");
        Value valObj = kvStore.get(key);
        if (valObj == null) {
            Transaction activeTxn = activeTransactionsById.get(xact);
            Value newVal = new Value(key, value, activeTxn.getTimestamp());
            kvStore.put(key, newVal);
            activeTxn.addToLog(newVal.getBeforeWriteTimestamp(xact));
        } else {
            rollback(xact);
            throw new Exception("Key: " + key + " already exists.");
        }
    }

    // return value of object key in transaction xact
    public static int read(int xact, int key) throws Exception {
        Value val = kvStore.get(key);
        if (val == null) {
            throw new Exception("There is no tuple with key " + key);
        }
        Transaction txn = activeTransactionsById.get(xact);
        ValueVersion versioned = val.getBeforeWriteTimestamp(txn.getTimestamp());
        if (versioned.getRts() < txn.getTimestamp()) {
            versioned.setRts(txn.getTimestamp());
        }
        versioned.addDependant(xact);
        txn.waitOne();
        logger.info("Read issued for " + xact + " => {" + key + "," + versioned.getContent()  + "}");
        return versioned.getContent();
    }

    // write value of existing object identified by key in transaction xact
    public static void write(int xact, int key, int value) throws Exception {
        logger.info("Write issued for " + xact + " => {" + key + "," + value + "}");
        Value val = kvStore.get(key);
        if (val == null) {
            System.out.println("Cannot issue write for a non existent key: " + key);
            rollback(xact);
        }
        Transaction txn = activeTransactionsById.get(xact);
        if (txn == null) {
            throw new Exception("Transaction " + xact + " is not active!");
        }
        ValueVersion versioned = val.getBeforeWriteTimestamp(txn.getTimestamp());
        if (versioned == null || txn.getTimestamp() < versioned.getRts()) {
            rollback(xact);
        } else if (txn.getTimestamp() >= versioned.getRts() && txn.getTimestamp() == versioned.getWts()) {
            versioned.setContent(value);
            //txn.addToLog(versioned);
        } else if (txn.getTimestamp() >= versioned.getRts() && versioned.getWts() < txn.getTimestamp()) {
            ValueVersion vers = val.createNewVersion(value, txn.getTimestamp(), txn.getTimestamp());
            txn.addToLog(vers);
        }
    }

    public static void commit(int xact) throws Exception {
        logger.info("Commit issued for " + xact);
        Transaction txn = activeTransactionsById.get(xact);
        if (txn == null) {
            throw new Exception("Transaction " + xact + " is not active.");
        }
        if (!txn.isWaiting()) {
            List<ValueVersion> txnLog = txn.getLog();
            for (ValueVersion writtenByThis: txnLog) {
                for (Integer possibleWaiter: writtenByThis.getDependants()) {
                    Transaction waiter = activeTransactionsById.get(possibleWaiter);
                    if (waiter != null) {
                        assert waiter.isWaiting();
                        waiter.waitLess();
                        if (!waiter.isWaiting()) {
                            commit(waiter.getTimestamp());
                        }
                    }
                }
            }
            activeTransactionsById.remove(xact);
        }
    }

    public static void rollback(int xact) throws Exception {
        Queue<Integer> rollbackQueue = new LinkedBlockingDeque<>();
        rollbackQueue.add(xact);
        while (!rollbackQueue.isEmpty()) {
            int txnId = rollbackQueue.remove();
            logger.info("Rollback " + txnId);
            Transaction txn = activeTransactionsById.remove(txnId);
            if (txn == null) {
                System.out.println("Transaction " + txnId + " was already rolled back!");
                continue;
            }
            LinkedList<ValueVersion> txnLog = txn.getLog();
            for (ValueVersion writtenByThis: txnLog) {
                int key = writtenByThis.getKey();
                kvStore.get(key).removeVersion(writtenByThis.getWts());
                for (Integer dependantTxnId : writtenByThis.getDependants()) {
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
        ValueVersion initialVersion = new ValueVersion(key, timestamp, 0, initialValue);
        this.versions.add(initialVersion);
    }

    ValueVersion createNewVersion(int content, int rts, int wts) {
        ValueVersion versioned = new ValueVersion(key, rts, wts, content);
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
        return versioned;
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
        Iterator<ValueVersion> it = versions.descendingIterator();
        while (it.hasNext()) {
            ValueVersion current = it.next();
            if (wts > current.getWts()) {
                return null;
            } else if (wts == current.getWts()) {
                it.remove();
                return current;
            }
        }
        return null;
    }

}

class ValueVersion {
    private final int key;
    private int rts;
    private int wts;
    private int content;
    private LinkedList<Integer> dependants = new LinkedList<>();

    ValueVersion(int key, int rts, int wts, int content) {
        this.key = key;
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

    public int getKey() {
        return this.key;
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
    private LinkedList<ValueVersion> writtenByThis = new LinkedList<>();
    private int waitingFor;

    public Transaction(int timestamp) {
        this.timestamp = timestamp;
        this.waitingFor = 0;
    }

    public int getTimestamp() {
        return this.timestamp;
    }

    public void addToLog(ValueVersion key) {
        writtenByThis.add(key);
    }

    public LinkedList<ValueVersion> getLog() {
        return this.writtenByThis;
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

