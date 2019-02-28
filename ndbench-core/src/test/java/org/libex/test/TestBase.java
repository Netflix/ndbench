package org.libex.test;

import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.libex.test.google.NullPointerTester;
import org.libex.test.rules.CheckableErrorCollector;

import javax.annotation.Nullable;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;

/**
 * JUnit test base class.
 * 
 * @author John Butler
 */
public abstract class TestBase {

    protected NullPointerTester nullPointerTester = new NullPointerTester();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public CheckableErrorCollector errorCollector = new CheckableErrorCollector();

    /**
     * Sets the {@code expectedException} to expect an exception of the provided {@code type} and with a superstring of
     * the provided {@code substring}
     * 
     * @param type
     *            the type of Exception to expect
     * @param substring
     *            a substring of the exception message to expect
     */
    protected void expectException(final Class<? extends Throwable> type, @Nullable final String substring) {
        expectedException.expect(type);
        if (substring != null) {
            expectedException.expectMessage(substring);
        }
    }

    protected void expectException(final Class<? extends Throwable> type, @Nullable final String substring, final Exception cause) {
        expectException(type, substring);
        expectedException.expectCause(sameInstance(cause));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void expectException(final Class<? extends Throwable> type, @Nullable final String substring,
            final Class<? extends Throwable> cause) {
        expectException(type, substring);
        expectedException.expectCause(instanceOf(cause));
    }
}
