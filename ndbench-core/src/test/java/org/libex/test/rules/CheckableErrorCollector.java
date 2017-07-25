package org.libex.test.rules;

import org.junit.rules.ErrorCollector;
import org.junit.runners.model.MultipleFailureException;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.ThreadSafe;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

@ParametersAreNonnullByDefault
@ThreadSafe
public class CheckableErrorCollector extends ErrorCollector {

    private List<Throwable> errors = newArrayList();

    public CheckableErrorCollector() {
    }

    public boolean containsErrors() {
        return !errors.isEmpty();
    }

    public boolean doesNotContainErrors() {
        return errors.isEmpty();
    }

    @Override
    public void verify() throws Throwable {
        MultipleFailureException.assertEmpty(errors);
        super.verify();
    }

    @Override
    public void addError(Throwable error) {
        errors.add(error);
        super.addError(error);
    }
}
