package org.libex.logging.log4j;

import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.libex.hamcrest.IsThrowable;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utilities on {@link LoggingEvent}
 *
 * @author John Butler
 *
 */
@NotThreadSafe
@ParametersAreNonnullByDefault
public final class LoggingEventsEx {

    /**
     * Creates a {@link Predicate} that matches a {@link LoggingEvent} that has
     * the specified level
     *
     * @param level
     *            the level to match
     * @return a {@link Predicate} that matches a {@link LoggingEvent} that has
     *         the specified level
     */
    public static Predicate<LoggingEvent> withLevel(final Level level) {
        checkNotNull(level);

        return withLevel(Matchers.equalTo(level));
    }

    /**
     * Creates a {@link Predicate} that matches a {@link LoggingEvent} whose
     * level matches the passed matcher
     *
     * @param matcher
     *            the matcher to use
     * @return a {@link Predicate} that matches a {@link LoggingEvent} whose
     *         level matches the passed matcher
     */
    public static Predicate<LoggingEvent> withLevel(
            final Matcher<? super Level> matcher) {
        checkNotNull(matcher);

        return event -> event != null && matcher.matches(event.getLevel());
    }

    public static Predicate<LoggingEvent> withRenderedMessage(
            final String message) {
        checkNotNull(message);

        return withRenderedMessage(Matchers.equalTo(message));
    }

    public static Predicate<LoggingEvent> withRenderedMessage(
            final Matcher<? super String> matcher) {
        return event -> event != null
                && matcher.matches(event.getRenderedMessage());
    }

    public static Predicate<LoggingEvent> withThrowable(
            final Class<? extends Throwable> type) {
        checkNotNull(type);

        return withThrowable(IsThrowable.isThrowableOfType(type));
    }

    public static Predicate<LoggingEvent> withThrowable(
            final Matcher<?> matcher)
    {
        return event -> event != null
                && matcher
                        .matches((event.getThrowableInformation() == null) ? null
                                : event.getThrowableInformation()
                                        .getThrowable());
    }


    private static final Function<LoggingEvent, String> TO_MESSAGE = event -> (String) event.getMessage();

    public static Function<LoggingEvent, String> toMessage() {
        return TO_MESSAGE;
    }

    private LoggingEventsEx() {
    }
}
