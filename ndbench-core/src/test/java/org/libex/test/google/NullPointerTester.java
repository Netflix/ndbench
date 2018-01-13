package org.libex.test.google;

import com.google.common.base.Converter;
import com.google.common.base.Objects;
import com.google.common.collect.*;
import com.google.common.reflect.Invokable;
import com.google.common.reflect.Parameter;
import com.google.common.reflect.Reflection;
import com.google.common.reflect.TypeToken;
import com.google.common.testing.ArbitraryInstances;
import com.google.common.testing.NullPointerTester.Visibility;
import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import javax.annotation.Nullable;
import java.lang.reflect.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is a modified version of {@link com.google.common.testing.NullPointerTester} to enable configuration
 * of the acceptable exception types thrown when {@code null} is provided to a
 * constructor or method
 *
 * @see com.google.common.testing.NullPointerTester
 */
public class NullPointerTester {

    private final ClassToInstanceMap<Object> defaults =
        MutableClassToInstanceMap.create();
    private final List<Member> ignoredMembers = Lists.newArrayList();

    private ExceptionTypePolicy policy = ExceptionTypePolicy.NPE_OR_UOE;

    public <T> NullPointerTester setDefault(final Class<T> type, final T value) {
      defaults.putInstance(type, checkNotNull(value));
      return this;
    }

    public NullPointerTester ignore(final Method method) {
      ignoredMembers.add(checkNotNull(method));
      return this;
    }

    public NullPointerTester policy(final ExceptionTypePolicy policy) {
      this.policy = policy;
      return this;
    }

    public void testConstructors(
            final Class<?> c,
            final Visibility minimalVisibility)
    {
        for (Constructor<?> constructor : c.getDeclaredConstructors()) {
            if (convert(minimalVisibility).isVisible(constructor) && !isIgnored(constructor)) {
                testConstructor(constructor);
            }
        }
    }

    private VisibilityLocal convert(
            final Visibility visibility)
    {
        return VisibilityLocal.valueOf(visibility.name());
    }

    public void testAllPublicConstructors(final Class<?> c) {
      testConstructors(c, Visibility.PUBLIC);
    }

    public void testStaticMethods(final Class<?> c, final Visibility minimalVisibility) {
        for (Method method : convert(minimalVisibility).getStaticMethods(c)) {
        if (!isIgnored(method)) {
          testMethod(null, method);
        }
      }
    }

    public void testAllPublicStaticMethods(final Class<?> c) {
      testStaticMethods(c, Visibility.PUBLIC);
    }

    public void testInstanceMethods(final Object instance, final Visibility minimalVisibility) {
      for (Method method : getInstanceMethodsToTest(instance.getClass(), minimalVisibility)) {
        testMethod(instance, method);
      }
    }

    ImmutableList<Method> getInstanceMethodsToTest(final Class<?> c, final Visibility minimalVisibility) {
      ImmutableList.Builder<Method> builder = ImmutableList.builder();
        for (Method method : convert(minimalVisibility).getInstanceMethods(c)) {
        if (!isIgnored(method)) {
          builder.add(method);
        }
      }
      return builder.build();
    }

    public void testAllPublicInstanceMethods(final Object instance) {
      testInstanceMethods(instance, Visibility.PUBLIC);
    }

    public void testMethod(@Nullable final Object instance, final Method method) {
      Class<?>[] types = method.getParameterTypes();
      for (int nullIndex = 0; nullIndex < types.length; nullIndex++) {
        testMethodParameter(instance, method, nullIndex);
      }
    }

    public void testConstructor(final Constructor<?> ctor) {
      Class<?> declaringClass = ctor.getDeclaringClass();
      checkArgument(Modifier.isStatic(declaringClass.getModifiers())
          || declaringClass.getEnclosingClass() == null,
          "Cannot test constructor of non-static inner class: %s", declaringClass.getName());
      Class<?>[] types = ctor.getParameterTypes();
      for (int nullIndex = 0; nullIndex < types.length; nullIndex++) {
        testConstructorParameter(ctor, nullIndex);
      }
    }

    public void testMethodParameter(
        @Nullable final Object instance, final Method method, final int paramIndex) {
      method.setAccessible(true);
      testParameter(instance, invokable(instance, method), paramIndex, method.getDeclaringClass());
    }

    public void testConstructorParameter(final Constructor<?> ctor, final int paramIndex) {
      ctor.setAccessible(true);
      testParameter(null, Invokable.from(ctor), paramIndex, ctor.getDeclaringClass());
    }

    /** Visibility of any method or constructor. */
    public enum VisibilityLocal {

      PACKAGE {
        @Override boolean isVisible(final int modifiers) {
          return !Modifier.isPrivate(modifiers);
        }
      },

      PROTECTED {
        @Override boolean isVisible(final int modifiers) {
          return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
        }
      },

      PUBLIC {
        @Override boolean isVisible(final int modifiers) {
          return Modifier.isPublic(modifiers);
        }
      };

      abstract boolean isVisible(final int modifiers);

      final boolean isVisible(final Member member) {
        return isVisible(member.getModifiers());
      }

      final Iterable<Method> getStaticMethods(final Class<?> cls) {
        ImmutableList.Builder<Method> builder = ImmutableList.builder();
        for (Method method : getVisibleMethods(cls)) {
          if (Invokable.from(method).isStatic()) {
            builder.add(method);
          }
        }
        return builder.build();
      }

      final Iterable<Method> getInstanceMethods(final Class<?> cls) {
        ConcurrentMap<Signature, Method> map = Maps.newConcurrentMap();
        for (Method method : getVisibleMethods(cls)) {
          if (!Invokable.from(method).isStatic()) {
            map.putIfAbsent(new Signature(method), method);
          }
        }
        return map.values();
      }

      private ImmutableList<Method> getVisibleMethods(final Class<?> cls) {
        // Don't use cls.getPackage() because it does nasty things like reading
        // a file.
        String visiblePackage = Reflection.getPackageName(cls);
        ImmutableList.Builder<Method> builder = ImmutableList.builder();
        for (Class<?> type : TypeToken.of(cls).getTypes().classes().rawTypes()) {
          if (!Reflection.getPackageName(type).equals(visiblePackage)) {
            break;
          }
          for (Method method : type.getDeclaredMethods()) {
            if (!method.isSynthetic() && isVisible(method)) {
              builder.add(method);
            }
          }
        }
        return builder.build();
      }
    }

    // TODO(benyu): Use labs/reflect/Signature if it graduates.
    private static final class Signature {
      private final String name;
      private final ImmutableList<Class<?>> parameterTypes;

      Signature(final Method method) {
        this(method.getName(), ImmutableList.copyOf(method.getParameterTypes()));
      }

      Signature(final String name, final ImmutableList<Class<?>> parameterTypes) {
        this.name = name;
        this.parameterTypes = parameterTypes;
      }

      @Override public boolean equals(final Object obj) {
        if (obj instanceof Signature) {
          Signature that = (Signature) obj;
          return name.equals(that.name)
              && parameterTypes.equals(that.parameterTypes);
        }
        return false;
      }

      @Override public int hashCode() {
        return Objects.hashCode(name, parameterTypes);
      }
    }

    private void testParameter(final Object instance, final Invokable<?, ?> invokable,
        final int paramIndex, final Class<?> testedClass) {
      if (isPrimitiveOrNullable(invokable.getParameters().get(paramIndex))) {
        return; // there's nothing to test
      }
      Object[] params = buildParamList(invokable, paramIndex);
      try {
        @SuppressWarnings("unchecked") // We'll get a runtime exception if the type is wrong.
        Invokable<Object, ?> unsafe = (Invokable<Object, ?>) invokable;
        unsafe.invoke(instance, params);
        Assert.fail("No exception thrown for parameter at index " + paramIndex
            + " from " + invokable + Arrays.toString(params) + " for " + testedClass);
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (policy.isExpectedType(cause)) {
          return;
        }
        AssertionFailedError error = new AssertionFailedError(
            "wrong exception thrown from " + invokable + ": " + cause);
        error.initCause(cause);
        throw error;
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    private Object[] buildParamList(final Invokable<?, ?> invokable, final int indexOfParamToSetToNull) {
      ImmutableList<Parameter> params = invokable.getParameters();
      Object[] args = new Object[params.size()];

      for (int i = 0; i < args.length; i++) {
        Parameter param = params.get(i);
        if (i != indexOfParamToSetToNull) {
          args[i] = getDefaultValue(param.getType());
          Assert.assertTrue(
              "Can't find or create a sample instance for type '"
                  + param.getType()
                  + "'; please provide one using NullPointerTester.setDefault()",
              args[i] != null || isNullable(param));
        }
      }
      return args;
    }

    private <T> T getDefaultValue(final TypeToken<T> type) {
      // We assume that all defaults are generics-safe, even if they aren't,
      // we take the risk.
      @SuppressWarnings("unchecked")
      T defaultValue = (T) defaults.getInstance(type.getRawType());
      if (defaultValue != null) {
        return defaultValue;
      }
      @SuppressWarnings("unchecked") // All arbitrary instances are generics-safe
      T arbitrary = (T) ArbitraryInstances.get(type.getRawType());
      if (arbitrary != null) {
        return arbitrary;
      }
      if (type.getRawType() == Class.class) {
        // If parameter is Class<? extends Foo>, we return Foo.class
        @SuppressWarnings("unchecked")
        T defaultClass = (T) getFirstTypeParameter(type.getType()).getRawType();
        return defaultClass;
      }
      if (type.getRawType() == TypeToken.class) {
        // If parameter is TypeToken<? extends Foo>, we return TypeToken<Foo>.
        @SuppressWarnings("unchecked")
        T defaultType = (T) getFirstTypeParameter(type.getType());
        return defaultType;
      }
      if (type.getRawType() == Converter.class) {
        TypeToken<?> convertFromType = type.resolveType(
            Converter.class.getTypeParameters()[0]);
        TypeToken<?> convertToType = type.resolveType(
            Converter.class.getTypeParameters()[1]);
        @SuppressWarnings("unchecked") // returns default for both F and T
        T defaultConverter = (T) defaultConverter(convertFromType, convertToType);
        return defaultConverter;
      }
      if (type.getRawType().isInterface()) {
        return newDefaultReturningProxy(type);
      }
      return null;
    }

    private <F, T> Converter<F, T> defaultConverter(
        final TypeToken<F> convertFromType, final TypeToken<T> convertToType) {
      return new Converter<F, T>() {
        @Override protected T doForward(final F a) {
          return doConvert(convertToType);
        }
        @Override protected F doBackward(final T b) {
          return doConvert(convertFromType);
        }

        private /*static*/ <S> S doConvert(final TypeToken<S> type) {
          return checkNotNull(getDefaultValue(type));
        }
      };
    }

    private static TypeToken<?> getFirstTypeParameter(final Type type) {
      if (type instanceof ParameterizedType) {
        return TypeToken.of(
            ((ParameterizedType) type).getActualTypeArguments()[0]);
      } else {
        return TypeToken.of(Object.class);
      }
    }

    private <T> T newDefaultReturningProxy(final TypeToken<T> type) {
      return new DummyProxy() {
        @Override <R> R dummyReturnValue(final TypeToken<R> returnType) {
          return getDefaultValue(returnType);
        }
      }.newProxy(type);
    }

    private static Invokable<?, ?> invokable(@Nullable final Object instance, final Method method) {
      if (instance == null) {
        return Invokable.from(method);
      } else {
        return TypeToken.of(instance.getClass()).method(method);
      }
    }

    static boolean isPrimitiveOrNullable(final Parameter param) {
      return param.getType().getRawType().isPrimitive() || isNullable(param);
    }

    private static boolean isNullable(final Parameter param) {
      return param.isAnnotationPresent(Nullable.class);
    }

    private boolean isIgnored(final Member member) {
      return member.isSynthetic() || ignoredMembers.contains(member);
    }

    /**
     * Strategy for exception type matching used by {@link NullPointerTester}.
     */
    public enum ExceptionTypePolicy {

      /**
       * Exceptions should be {@link NullPointerException} or
       * {@link UnsupportedOperationException}.
       */
      NPE_OR_UOE() {
        @Override
        public boolean isExpectedType(final Throwable cause) {
          return cause instanceof NullPointerException
              || cause instanceof UnsupportedOperationException;
        }
      },

      /**
       * Exceptions should be {@link NullPointerException},
       * {@link IllegalArgumentException}, or
       * {@link UnsupportedOperationException}.
       */
      NPE_IAE_OR_UOE() {
        @Override
        public boolean isExpectedType(final Throwable cause) {
          return cause instanceof NullPointerException
              || cause instanceof IllegalArgumentException
              || cause instanceof UnsupportedOperationException;
        }
      };

      public abstract boolean isExpectedType(final Throwable cause);
    }
}
