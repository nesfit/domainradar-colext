package cz.vut.fit.domainradar;

/**
 * A functional interface that represents a consumer operation that accepts three input arguments.
 *
 * @param <T> The type of the first argument to the operation.
 * @param <U> The type of the second argument to the operation.
 * @param <V> The type of the third argument to the operation.
 * @author Matěj Čech
 */
@FunctionalInterface
public interface TriConsumer<T, U, V> {
    /**
     * Performs the operation on the given arguments.
     *
     * @param t The first input argument.
     * @param u The second input argument.
     * @param v The third input argument.
     */
    void accept(T t, U u, V v);
}