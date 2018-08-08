package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util;

import java.util.Iterator;
import java.util.function.Supplier;

public class InfiniteIterator<T> implements Iterator<T> {

    private final Supplier<T> supplier;

    public InfiniteIterator(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public boolean hasNext() {
        return true;
    }

    public T next() {
        return supplier.get();
    }

}
