package org.libex.hamcrest;

import com.google.common.base.Optional;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.ThreadSafe;

import static org.hamcrest.CoreMatchers.instanceOf;

@ParametersAreNonnullByDefault
@ThreadSafe
public class IsThrowable<T extends Throwable> extends BaseMatcher<Object> {

    public static <T extends Throwable> IsThrowable<T> isThrowableOfType(
            Class<T> type) {
        return new IsThrowable<T>(Optional.of(type), Optional.<String> absent());
    }

    public static IsThrowable<Throwable> isThrowableWithMessage(String message) {
        return new IsThrowable<Throwable>(Optional.<Class<Throwable>> absent(),
                Optional.of(message));
    }

    public static <T extends Throwable> IsThrowable<T> isThrowable(
            Class<T> type, String message) {
        return new IsThrowable<T>(Optional.of(type), Optional.of(message));
    }

    @Nullable
    private final Matcher<Object> type;
    @Nullable
    private final Matcher<String> message;

    private IsThrowable(Optional<Class<T>> type, Optional<String> message) {
        super();
        this.type = type.isPresent() ? instanceOf(type.get()) : null;
        this.message = message.isPresent() ? Matchers.containsString(message
                .get()) : null;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("A throwable");
        if (type != null) {
            description.appendText(" of type matching ");
            description.appendDescriptionOf(type);
        }

        if (message != null) {
            if (type != null)
                description.appendText("and");
            description.appendText(" with message matching ");
            description.appendDescriptionOf(message);
        }
    }

    @Override
    public boolean matches(Object arg0) {
        boolean result = arg0 instanceof Throwable;
        if (result) {
            Throwable t = (Throwable) arg0;
            if (type != null) {
                result &= type.matches(t);
            }

            if (message != null) {
                if (arg0 == null)
                    result = false;
                else
                    result &= message.matches(t.getMessage());
            }
        }

        return result;
    }

}
