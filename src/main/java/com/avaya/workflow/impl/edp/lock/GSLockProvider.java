package com.avaya.workflow.impl.edp.lock;

import org.openspaces.core.space.SpaceProxyConfigurer;

import com.avaya.collaboration.datagrid.api.user.UserInfo;
import com.avaya.collaboration.datagrid.api.util.DCMUtil;
import com.avaya.workflow.lock.LockProvider;
import com.avaya.workflow.logger.Logger;
import com.avaya.workflow.logger.LoggerFactory;

public class GSLockProvider implements LockProvider {
    private static GSLockProvider gslockProvider = null;
    private static final int LOOKUP_TIMEOUT = 10000; // 10 seconds.
    private static final long DEFAULT_LOCK_TIME_TO_LIVE = 10000; // 10s
    private static final long DEFAULT_WAITING_FOR_LOCK_TIME = 11000; // 11s
    private static Logger logger = LoggerFactory.getLogger(GSLockProvider.class);
    private static final ClassLoader gsCl = GSLockProvider.class.getClassLoader();
    private static GSLockManager lockManager;
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
        lockManager = new GSLockManager(spaceProxyConfigurer);
    }

    @Override
    public boolean tryLock(String instanceId) {
        boolean result = false;
        ClassLoader tcl = null;

        try {
            tcl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(gsCl);
            // lockTimeToLive is DEFAULT_LOCK_TIME_TO_LIVE milliseconds;
            // The waitingForLockTimeout is 0 second.
            result = lockManager.lock(instanceId, DEFAULT_LOCK_TIME_TO_LIVE, 0);
        } catch (Exception e) {
            // Cannot lock the key.
            logger.error("tryLock exception: ", e);
        } finally {
            if (tcl != null) {
                Thread.currentThread().setContextClassLoader(tcl);
            }
        }

        if (result == false) {
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
        boolean result = false;
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
            result = lockManager.lock(instanceId, DEFAULT_LOCK_TIME_TO_LIVE, DEFAULT_WAITING_FOR_LOCK_TIME);
        } catch (Exception e) {
            // Cannot lock the key.
            logger.error("acquireLock exception: ", e);
        } finally {
            if (tcl != null) {
                Thread.currentThread().setContextClassLoader(tcl);
            }
        }

        if (result == false) {
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
            lockManager.unlock(instanceId);
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
        releaseLock(instanceId);
    }

    public boolean isLocked(String instanceId) {
        ClassLoader tcl = null;
        boolean result = false;
        try {
            tcl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(gsCl);
            result = lockManager.isLocked(instanceId);
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