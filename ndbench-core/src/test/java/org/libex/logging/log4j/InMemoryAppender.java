package org.libex.logging.log4j;

import com.google.common.collect.ImmutableList;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Appender that maintains the logging events in memory.
 *
 * @author John Butler
 */
@ThreadSafe
@ParametersAreNonnullByDefault
public class InMemoryAppender extends AppenderSkeleton {

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private volatile ImmutableList.Builder<LoggingEvent> eventListBuilder = ImmutableList
            .builder();

    @Override
    public void close() {
        // NO OP
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    protected void append(LoggingEvent event) {
        writeLock.lock();
        try {
            eventListBuilder.add(event);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @return the observed logging events
     */
    public ImmutableList<LoggingEvent> getLoggingEvents() {
        readLock.lock();
        try {
            return eventListBuilder.build();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Clears the list of observed logging events
     */
    public void clear() {
        writeLock.lock();
        try {
            eventListBuilder = ImmutableList.builder();
        } finally {
            writeLock.unlock();
        }
    }
}
