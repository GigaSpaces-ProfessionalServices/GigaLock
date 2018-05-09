package com.avaya.workflow.impl.edp.lock;

import com.gigaspaces.client.transaction.DistributedTransactionManagerProvider;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.OperationTimeoutException;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.TransactionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.SpaceTimeoutException;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides a distributed lock, using built-in transaction locking.
 *
 * @author Alon Shoham
 */
public class GSLockManager {
    private final static Log logger = LogFactory.getLog(GSLockManager.class);

    private final IJSpace space;
    private final ConcurrentHashMap<Serializable, LockInfo> locksCache = new ConcurrentHashMap<Serializable, LockInfo>();
    private final DistributedTransactionManagerProvider transactionManagerProvider;

    public GSLockManager(SpaceProxyConfigurer spaceProxyConfigurer) {
        this.space = spaceProxyConfigurer.create();
        try {
            transactionManagerProvider = new DistributedTransactionManagerProvider();
        } catch (TransactionException e) {
            throw new GSLockException("Failed to obtain transaction lock manager", e);
        }
    }

    public boolean lock(Serializable key, long lockTimeToLive, long waitingForLockTime) {
        final Transaction tr = getTransaction(lockTimeToLive);

        try {
            space.write(new GSLockEntry(key), tr, Lease.FOREVER, waitingForLockTime, Modifiers.UPDATE_OR_WRITE);
            synchronized (key) {
                locksCache.put(key, new LockInfo(tr, lockTimeToLive));
            }
            System.out.println("acquired lock for " + key);
            return true;
        } catch (OperationTimeoutException e) {
            try {
                tr.abort();
            } catch (Exception re) {
                logger.warn("Failed to abort transaction", re);
            }
            return false;
        } catch (Throwable t) {
            try {
                tr.abort();
            } catch (Exception re) {
                logger.warn("Failed to abort transaction", re);
            }
            throw new GSLockException("Failed to obtain lock for key [" + key + "]", t);
        }
    }

    public void unlock(Serializable key) {
        final LockInfo lockInfo = locksCache.get(key);

        if (lockInfo != null) {
            try {
                lockInfo.tx.abort();
                synchronized (key) {
                    if (locksCache.get(key) == lockInfo)
                        locksCache.remove(key);
                }
            } catch (Exception e) {
                throw new GSLockException("Failed to release lock for entry " + key, e);
            }
        } else {
            if (isEntryLockedRemotely(key)) {
                throw new GSLockException("Cannot release entry which is locked by another client");
            }
        }
    }

    public boolean isLocked(Serializable key) {
        //Looks locally for the locked object
        if (isObjectLockedLocally(key))
            return true;
        //Looks remotely for the locked object
        if (isEntryLockedRemotely(key))
            return true;

        return false;
    }

    private boolean isEntryLockedRemotely(Serializable key) {
        try {
            return space.read(new GSLockEntry(key), null, 0, Modifiers.DIRTY_READ) != null;
        } catch (Exception e) {
            throw new GSLockException("failed to determine if entry " + key + " is locked by another client", e);
        }
    }

    private boolean isObjectLockedLocally(Serializable key) {
        LockInfo lockInfo = locksCache.get(key);

        return lockInfo != null ? lockInfo.isTxAlive() : false;
    }

    private Transaction getTransaction(long lockTimeToLive) {
        try {
            return TransactionFactory.create(transactionManagerProvider.getTransactionManager(), lockTimeToLive).transaction;
        } catch (Exception e) {
            throw new GSLockException("Failed to create lock transaction", e);
        }
    }

    private class LockInfo {
        final Transaction tx;
        private final long expirationTime;


        public LockInfo(Transaction tx, long timeout) {
            this.tx = tx;
            this.expirationTime = System.currentTimeMillis() + timeout;
        }

        public boolean isTxAlive() {
            return System.currentTimeMillis() < expirationTime;
        }
    }
}