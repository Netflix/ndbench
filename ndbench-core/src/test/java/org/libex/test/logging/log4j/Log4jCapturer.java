package org.libex.test.logging.log4j;

import com.google.common.collect.ImmutableList;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggingEvent;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.StringDescription;
import org.hamcrest.collection.IsIterableWithSize;
import org.hamcrest.core.IsAnything;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.libex.hamcrest.IsThrowable;
import org.libex.logging.log4j.InMemoryAppender;
import org.libex.logging.log4j.LoggingEventsEx;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.libex.logging.log4j.LoggingEventsEx.toMessage;

/**
 *
 * Source: https://raw.githubusercontent.com/dancerjohn/LibEx/master/testlibex/src/main/java/org/libex/test/logging/log4j/Log4jCapturer.java
 * Not yet available in standard maven repo's, so including source here.
 *
 *
 * Rule that allows for capturing Log4J logging for test verification.
 *
 * @author John Butler
 *
 */
@NotThreadSafe
@ParametersAreNonnullByDefault
public class Log4jCapturer implements TestRule {

    private static final String DEFAULT_LAYOUT = "%d{DATE} %5p %C{1}.%M(),%L - %m%n";
    private static final String APPENDER_NAME = "Log4jCapturerAppender";

    /**
     * @return new capturer builder
     */
    public static Log4jCapturerBuilder builder() {
        return new Log4jCapturerBuilder();
    }

    /**
     * Builder for {@link Log4jCapturer}
     */
    public static class Log4jCapturerBuilder {
        // private Logger logger = Logger.getRootLogger();
        private List<Logger> loggers = newArrayList();
        private Level threshold = Level.INFO;
        private Layout layout = new PatternLayout(DEFAULT_LAYOUT);

        /**
         * Sets the logging threshold for messages that should be recorded. This
         * is set as the threshold on the created {@link Appender}
         *
         * @param threshold
         *            the lowest level of messages that should be held
         * @return this instance
         *
         * @see AppenderSkeleton#setThreshold(Priority)
         */
        public Log4jCapturerBuilder setThreshold(final Level threshold) {
            this.threshold = threshold;
            return this;
        }

        /**
         * Sets the logging layout for message that are recorded. This is set as
         * the layout on the created {@link Appender}
         *
         * @param layout
         *            the layout to set
         * @return this instance
         *
         * @see AppenderSkeleton#setLayout(Layout)
         */
        public Log4jCapturerBuilder setLayout(final Layout layout) {
            this.layout = layout;
            return this;
        }

        /**
         * Add the logger for messages that are recorded.
         *
         * @param logger
         *            the logger to add
         * @return this instance
         */
        public Log4jCapturerBuilder addLogger(
                final String logger)
        {
            this.loggers.add(Logger.getLogger(logger));
            return this;
        }

        /**
         * @return a new {@link Log4jCapturer}
         */
        public Log4jCapturer build() {
            return new Log4jCapturer(threshold, layout, loggers);
        }
    }

    private final InMemoryAppender appender;
    private final List<Logger> loggers;

    private Log4jCapturer(
            final Level threshold,
            final Layout layout,
            final List<Logger> loggers) {
        appender = new InMemoryAppender();
        appender.setThreshold(threshold);
        appender.setLayout(layout);
        appender.setName(APPENDER_NAME);
        this.loggers = (loggers.isEmpty()) ? newArrayList(Logger.getRootLogger()) : ImmutableList.copyOf(loggers);

        for (Logger logger : this.loggers) {
            logger.setLevel(threshold);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.junit.rules.TestRule#apply(org.junit.runners.model.Statement,
     * org.junit.runner.Description)
     */
    @Override
    public Statement apply(final Statement statement, final Description description) {
        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                addAppender();

                try {
                    statement.evaluate();
                } finally {
                    removeAppender();
                }
            }
        };
    }

    private void addAppender() {
        appender.clear();

        for (Logger logger : loggers) {
            logger.addAppender(appender);
        }
    }

    private void removeAppender() {
        try {
            appender.clear();
            for (Logger logger : loggers) {
                logger.removeAppender(appender);
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    /**
     * Clears the list of currently recorded logs.
     */
    public void clearLog() {
        appender.clear();
    }

    /**
     * Gets the list of logs that matches the passed assertion
     *
     * @param assertion
     *            the filter by which to retrieve logs
     * @return an unmodifiable Iterable over the list of logs that match the
     *         passed assertion
     */
    private Stream<LoggingEvent> filter(final Predicate<LoggingEvent> assertion) {
        return appender.getLoggingEvents().stream().filter(assertion);
    }

    /**
     * Gets the list of logs that matches the passed assertion
     *
     * @param assertion
     *            the filter by which to retrieve logs
     * @return an unmodifiable Iterable over the list of logs that match the
     *         passed assertion
     */
    private Stream<LoggingEvent> getLogs(final LogAssertion assertion) {
        return filter(assertion.criteria());
    }

    /**
     * Gets the list of log messages for the logs that match the passed assertion
     *
     * @param assertion
     *            the filter by which to retrieve logs
     * @return an unmodifiable Iterable over the list of log messages for logs
     *         that match the passed assertion
     */
    public Iterable<String> getLogMessages(final LogAssertion assertion) {
        return getLogs(assertion).map(toMessage()).collect(Collectors.toList());
    }

    /**
     * Asserts the passed assertion
     *
     * @param assertion
     *            the logging assertion to verify
     */
    public void assertThat(final LogAssertion assertion) {

        List<LoggingEvent> logs = appender.getLoggingEvents();

        if (assertion.times <=1 ) {
            LoggingEvent event = logs.stream().filter(assertion.criteria()).findFirst().orElse(null);
            Matcher<Object> matcher = (assertion.logged) ? notNullValue()
                    : nullValue();
            MatcherAssert.assertThat(assertion.toString(), event, matcher);
        } else {
            MatcherAssert.assertThat(assertion.toString(),
                    logs.stream().filter(assertion.criteria()).collect(Collectors.toList()),
                    IsIterableWithSize.iterableWithSize(assertion.times));
        }

    }

    /**
     * Asserts that the passed substring was logged at the passed level
     *
     * @param level
     *          the expected level
     * @param substring
     *          the expected substring
     */
    public void assertRenderedMessageLogged(final Level level, final String substring) {
        assertThat(LogAssertion.newLogAssertion()
                .isLogged()
                .withLevel(level)
                .withRenderedMessage(substring));
    }

    /**
     * A LoggingEvent assertion
     */
    public static class LogAssertion {

        /**
         * @return a new empty assertion with default values
         */
        public static LogAssertion newLogAssertion() {
            return new LogAssertion();
        }

        private boolean logged = true;
        private int times = 1;
        private Matcher<? super Level> level = Matchers.anything();
        private Matcher<? super String> message = Matchers.anything();
        private Matcher<?> exception = Matchers.anything();

        /**
         * Sets the assertion to expect the message to be logged. This method
         * should be used in conjunction with one of the other {@code withX} methods. This method is mutually exclusive
         * with {@link #isNotLogged()}
         *
         * @return this instance
         */
        public LogAssertion isLogged() {
            return isLogged(1);
        }

        /**
         * Sets the assertion to expect the message to be logged. This method
         * should be used in conjunction with one of the other {@code withX} methods. This method is mutually exclusive
         * with {@link #isNotLogged()}
         *
         *  @param times
         *              the number of times to expect the message to be logged.
         *              Values 0 or greater are valid, If 0, will cause the
         *              expectation that the message was NOT logged
         * @return this instance
         */
        public LogAssertion isLogged(final int times) {
            checkArgument(times >= 0);

            if (times == 0) {
                return isNotLogged();
            } else {
                this.logged = true;
                this.times = times;
                return this;
            }
        }

        /**
         * Sets the assertion to expect the message to NOT be logged. This
         * method should be used in conjunction with one of the other {@code withX} methods. This method is mutually
         * exclusive with {@link #isLogged()}
         *
         * @return this instance
         */
        public LogAssertion isNotLogged() {
            this.logged = false;
            this.times = 0;
            return this;
        }

        /**
         * Sets the assertion to expect the message to have the passed {@code level}. The use of this method is
         * sufficient to assert a
         * message is logged. No other method calls are required, other than the
         * call to {@link Log4jCapturer#assertThat(LogAssertion)}.
         *
         * @param level
         *            the level to expect
         * @return this instance
         */
        public LogAssertion withLevel(final Level level) {
            return withLevel(Matchers.equalTo(level));
        }

        /**
         * Sets the assertion to expect the message to have a level that matches
         * the passed {@code level}. The use of this method is sufficient to
         * assert a message is logged. No other method calls are required, other
         * than the call to {@link Log4jCapturer#assertThat(LogAssertion)}.
         *
         * @param level
         *            the level to expect
         * @return this instance
         */
        public LogAssertion withLevel(final Matcher<? super Level> level) {
            this.level = level;
            return this;
        }

        /**
         * Sets the assertion to expect the rendered (formatted) message to have
         * a message that is super-string of the passed {@code substring}. The
         * use of this method is sufficient to assert a message is logged. No
         * other method calls are required, other than the call to {@link Log4jCapturer#assertThat(LogAssertion)}.
         *
         * @param substring
         *            the message to expect
         * @return this instance
         */
        public LogAssertion withRenderedMessage(final String substring) {
            return withRenderedMessage(Matchers.containsString(substring));
        }

        /**
         * Sets the assertion to expect the rendered (formatted) message to
         * match the passed {@code message}. The use of this method is
         * sufficient to assert a message is logged. No other method calls are
         * required, other than the call to {@link Log4jCapturer#assertThat(LogAssertion)}.
         *
         * @param message
         *            the message to expect
         * @return this instance
         */
        public LogAssertion withRenderedMessage(final Matcher<? super String> message) {
            this.message = message;
            return this;
        }

        /**
         * Sets the assertion to expect the logging event to contain an
         * exception that matches the passed {@code exception}. The use of this
         * method is sufficient to assert a message is logged. No other method
         * calls are required, other than the call to {@link Log4jCapturer#assertThat(LogAssertion)}.
         *
         * @param exception
         *            the exception matcher, consider {@link IsThrowable}
         * @return this instance
         */
        public LogAssertion withException(final Class<? extends Throwable> exception) {
            return withException(CoreMatchers.instanceOf(exception));
        }

        /**
         * Sets the assertion to expect the logging event to contain an
         * exception that matches the passed {@code exception}. The use of this
         * method is sufficient to assert a message is logged. No other method
         * calls are required, other than the call to {@link Log4jCapturer#assertThat(LogAssertion)}.
         *
         * @param exception
         *            the exception matcher, consider {@link IsThrowable}
         * @return this instance
         */
        public LogAssertion withException(final Matcher<?> exception)
        {
            this.exception = exception;
            return this;
        }

        @SuppressWarnings("unchecked")
        private Predicate<LoggingEvent> criteria() {
            return LoggingEventsEx.withLevel(level)
                    .and(LoggingEventsEx.withRenderedMessage(message))
                    .and(LoggingEventsEx.withThrowable(exception));
        }

        @Override
        public String toString() {
            org.hamcrest.Description description = new StringDescription();

            if (logged) {
                description.appendText("Message logged");
            } else {
                description.appendText("No message logged");
            }

            if (notIsAnything(level)) {
                description.appendText(" with level ");
                description.appendDescriptionOf(level);
            }

            if (notIsAnything(message)) {
                description.appendText(" with message ");
                description.appendDescriptionOf(message);
            }

            if (notIsAnything(exception)) {
                description.appendText(" with exception ");
                description.appendDescriptionOf(exception);
            }

            return description.toString();
        }

        private boolean notIsAnything(final Matcher<?> matcher) {
            return !(matcher instanceof IsAnything);
        }
    }
}
