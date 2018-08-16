package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.multi_threading.problem_3_partial_ordering.linealizer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;


public class Linealizer<K, R> {

    private final ConcurrentHashMap<K, Queue<LinearizedTask>> tasksByKeys = new ConcurrentHashMap<>();
    private final AtomicLong size = new AtomicLong();

    public LinearizedTask processOnKey(K key, Executor executor, Callable<R> callable) {
        LinearizedTask keyedTask = new LinearizedTask(key, executor, callable);
        tasksByKeys.compute(key, (k, queue) -> {
            if (queue == null) {
                // this is first message with this key, lets create the queue for messages
                // which can arrive when current task is in the middle of progress
                queue = new LinkedList<>();
                executor.execute(keyedTask);
                return queue;
            } else {
                queue.add(keyedTask);
                // return same queue, mapping in CHM stay unchanged
                return queue;
            }
        });
        size.incrementAndGet();
        return keyedTask;
    }

    public class LinearizedTask implements Runnable {
        private final K key;
        private final Executor executor;
        private final Callable<R> callable;
        private final CompletableFuture<R> future = new CompletableFuture<>();

        LinearizedTask(K key, Executor executor, Callable<R> callable) {
            this.key = key;
            this.executor = executor;
            this.callable = callable;
        }

        public CompletableFuture<R> getFuture() {
            return future;
        }

        @Override
        public void run() {
            try {
                R result = callable.call();
                future.complete(result);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                size.decrementAndGet();
                tasksByKeys.compute(key, (key, queue) -> {
                    LinearizedTask nextTask = queue.poll();
                    if (nextTask == null) {
                        // there is no tasks with same key, lets remove queue from CHM
                        return null;
                    }
                    try {
                        // schedule the next task with same key
                        nextTask.executor.execute(nextTask);
                    } finally {
                        return queue;
                    }
                });
            }
        }

        public void cancelAll() {
            size.decrementAndGet();
            future.cancel(false);
            tasksByKeys.compute(key, (key, queue) -> {
                while (true) {
                    LinearizedTask nextTask = queue.poll();
                    if (nextTask == null) {
                        // there is no tasks with same key, lets remove queue from CHM
                        return null;
                    }
                    size.decrementAndGet();
                    // schedule the next task with same key
                    nextTask.future.cancel(false);
                }
            });
        }

    }

    public long getSize() {
        return size.get();
    }
}
