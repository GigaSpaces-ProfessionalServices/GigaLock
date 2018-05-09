package com.avaya.workflow.impl.edp.lock;

import org.openspaces.core.GigaMap;
import org.openspaces.core.GigaMapConfigurer;
import org.openspaces.core.map.LockHandle;
import org.openspaces.core.map.MapConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;
import org.springframework.transaction.TransactionDefinition;

import com.avaya.collaboration.datagrid.api.user.UserInfo;
import com.avaya.collaboration.datagrid.api.util.DCMUtil;
import com.avaya.workflow.lock.LockProvider;
import com.avaya.workflow.logger.Logger;
import com.avaya.workflow.logger.LoggerFactory;
import com.j_spaces.map.IMap;

public class GSLockProvider implements LockProvider {
    private static GSLockProvider gslockProvider = null;
    private static final int LOOKUP_TIMEOUT = 10000; // 10 seconds.
    private static final long DEFAULT_LOCK_TIME_TO_LIVE = 10000; // 10s
    private static final long DEFAULT_WAITING_FOR_LOCK_TIME = 11000; // 11s
    private static Logger logger = LoggerFactory.getLogger(GSLockProvider.class);
    private static final ClassLoader gsCl = GSLockProvider.class.getClassLoader();
    private static IMap map = null;
    private static GigaMap gigaMap = null;
    private static SpaceProxyConfigurer spaceProxyConfigurer = null;

    public static final GSLockProvider getInstance() {
        if (gslockProvider == null) {
            synchronized (GSLockProvider.class) {
                if (gslockProvider == null) {
                    initializeService();
                }
            }
        }
        return gslockProvider;
    }

    private GSLockProvider() {
    }

    protected static void initializeService() {
        final String lookupLocators = DCMUtil.getInstance().getLookupLocatorsStr();
        final UserInfo userInfo = DCMUtil.getInstance().createUserInfo();
        spaceProxyConfigurer = new SpaceProxyConfigurer(DCMUtil.getPlatformSpaceName());

        gslockProvider = new GSLockProvider();
        if (userInfo != null) {
            // Map the DCM type userinfo to the Gigaspaces type
            spaceProxyConfigurer.credentials(userInfo.getUserName(), userInfo.getPassword());
        }

        spaceProxyConfigurer.lookupTimeout(LOOKUP_TIMEOUT);
        if (lookupLocators != null && lookupLocators.length() > 0) {
            spaceProxyConfigurer.lookupLocators(lookupLocators);
        }
        map = new MapConfigurer(spaceProxyConfigurer.space()).createMap();

        // By default the lock time to live is DEFAULT_LOCK_TIME_TO_LIVE
        // milliseconds and waiting for lock timeout is
        // DEFAULT_WAITING_FOR_LOCK_TIME milliseconds.
        gigaMap = new GigaMapConfigurer(map).defaultLockTimeToLive(DEFAULT_LOCK_TIME_TO_LIVE)
                .defaultWaitingForLockTimeout(DEFAULT_WAITING_FOR_LOCK_TIME)
                .defaultIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ).gigaMap();

    }

    @Override
    public boolean tryLock(String instanceId) {
        LockHandle lockHandle = null;
        ClassLoader tcl = null;

        try {
            tcl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(gsCl);
            // lockTimeToLive is DEFAULT_LOCK_TIME_TO_LIVE milliseconds;
            // The waitingForLockTimeout is 0 second.
            lockHandle = gigaMap.lock(instanceId, DEFAULT_LOCK_TIME_TO_LIVE, 0);
        } catch (Exception e) {
            // Cannot lock the key.
            logger.error("tryLock exception: ", e);
        } finally {
            if (tcl != null) {
                Thread.currentThread().setContextClassLoader(tcl);
            }
        }

        if (null == lockHandle) {
            if (logger.isFineEnabled()) {
                logger.fine("tryLock did not acquire the GSLock on instance: " + instanceId + " thread: "
                        + Thread.currentThread().getId());
            }
            return false;
        }

        if (logger.isFineEnabled()) {
            logger.fine("tryLock acquired the GSLock on instance: " + instanceId + " thread: "
                    + Thread.currentThread().getId());
        }

        return true;
    }

    @Override
    public void acquireLock(String instanceId) {
        LockHandle lockHandle = null;
        ClassLoader tcl = null;

        if (logger.isFineEnabled()) {
            logger.fine("Try acquiring GSLock on instance: " + instanceId + "; thread: "
                    + Thread.currentThread().getId());
        }

        try {
            tcl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(gsCl);
            // Use DEFAULT_LOCK_TIME_TO_LIVE and DEFAULT_WAITING_FOR_LOCK_TIME
            // explicitly even though they were set as default when creating the
            // GigaMap.
            lockHandle = gigaMap.lock(instanceId, DEFAULT_LOCK_TIME_TO_LIVE, DEFAULT_WAITING_FOR_LOCK_TIME);
        } catch (Exception e) {
            // Cannot lock the key.
            logger.error("acquireLock exception: ", e);
        } finally {
            if (tcl != null) {
                Thread.currentThread().setContextClassLoader(tcl);
            }
        }

        if (null == lockHandle) {
            logger.error("Failed to acquire GSLock on instance: " + instanceId + " thread: "
                    + Thread.currentThread().getId());
        } else {
            if (logger.isFineEnabled()) {
                logger.fine("Acquired GSLock on instance: " + instanceId + "; thread: "
                        + Thread.currentThread().getId());
            }
        }

    }

    @Override
    public void releaseLock(String instanceId) {
        ClassLoader tcl = null;

        try {
            tcl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(gsCl);
            gigaMap.unlock(instanceId);
        } catch (Exception e) {
            // Cannot release the key.
            logger.error("releaseLock exception: ", e);
        } finally {
            if (tcl != null) {
                Thread.currentThread().setContextClassLoader(tcl);
            }
        }

        if (logger.isFineEnabled()) {
            logger.fine("Released GSLock on instance: " + instanceId + "; thread:"
                    + Thread.currentThread().getId());
        }

    }

    @Override
    public void deleteLock(String instanceId) {
        ClassLoader tcl = null;

        try {
            tcl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(gsCl);
            // Just in case it was locked.
            gigaMap.unlock(instanceId);
        } catch (Exception e) {
            // Cannot unlock the key.
            logger.error("deleteLock exception: ", e);
        } finally {
            gigaMap.remove(instanceId);
            if (tcl != null) {
                Thread.currentThread().setContextClassLoader(tcl);
            }
        }

        if (logger.isFineEnabled()) {
            logger.fine("Deleted GSLock on instance: " + instanceId);
        }

    }

    public boolean isLocked(String instanceId) {
        ClassLoader tcl = null;
        boolean result = false;
        try {
            tcl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(gsCl);
            result = gigaMap.isLocked(instanceId);
        } catch (Exception e) {
            // Cannot check the lock.
            logger.error("isLocked exception: ", e);
        } finally {
            if (tcl != null) {
                Thread.currentThread().setContextClassLoader(tcl);
            }
        }

        return result;
    }

    @Override
    public void cleanup() {
        if (spaceProxyConfigurer != null) {
            logger.info("Close spaceProxyConfigurer used by the GigaMap lock.");
            spaceProxyConfigurer.close();
        }
    }
}