/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.common.util;

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
<<<<<<< HEAD
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Random;
=======

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
>>>>>>> 2346686c3b8621a585ad678926adf60206227367
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
<<<<<<< HEAD

import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
=======
import java.util.concurrent.TimeoutException;

>>>>>>> 2346686c3b8621a585ad678926adf60206227367
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * This class provides 2 things over the java {@link ScheduledExecutorService}.
 *
 * <p>1. It takes {@link SafeRunnable objects} instead of plain Runnable objects.
 * This means that exceptions in scheduled tasks wont go unnoticed and will be
 * logged.
 *
 * <p>2. It supports submitting tasks with an ordering key, so that tasks submitted
 * with the same key will always be executed in order, but tasks across
 * different keys can be unordered. This retains parallelism while retaining the
 * basic amount of ordering we want (e.g. , per ledger handle). Ordering is
 * achieved by hashing the key objects to threads by their {@link #hashCode()}
 * method.
 */
<<<<<<< HEAD
public class OrderedScheduler {
    public static final int NO_TASK_LIMIT = -1;
    protected static final long WARN_TIME_MICRO_SEC_DEFAULT = TimeUnit.SECONDS.toMicros(1);

    final String name;
    final ListeningScheduledExecutorService threads[];
    final long threadIds[];
    final Random rand = new Random();
    final OpStatsLogger taskExecutionStats;
    final OpStatsLogger taskPendingStats;
    final boolean traceTaskExecution;
    final long warnTimeMicroSec;
    final int maxTasksInQueue;
=======
public class OrderedScheduler extends OrderedExecutor implements ScheduledExecutorService {
>>>>>>> 2346686c3b8621a585ad678926adf60206227367

    /**
     * Create a builder to build ordered scheduler.
     *
     * @return builder to build ordered scheduler.
     */
    public static SchedulerBuilder newSchedulerBuilder() {
        return new SchedulerBuilder();
    }

    /**
     * Builder to build ordered scheduler.
     */
<<<<<<< HEAD
    public static class SchedulerBuilder extends AbstractBuilder<OrderedScheduler> {}

    /**
     * Abstract builder class to build {@link OrderedScheduler}.
     */
    public abstract static class AbstractBuilder<T extends OrderedScheduler> {
        protected String name = getClass().getSimpleName();
        protected int numThreads = Runtime.getRuntime().availableProcessors();
        protected ThreadFactory threadFactory = null;
        protected StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        protected boolean traceTaskExecution = false;
        protected long warnTimeMicroSec = WARN_TIME_MICRO_SEC_DEFAULT;
        protected int maxTasksInQueue = NO_TASK_LIMIT;

        public AbstractBuilder<T> name(String name) {
            this.name = name;
            return this;
        }

        public AbstractBuilder<T> numThreads(int num) {
            this.numThreads = num;
            return this;
        }

        public AbstractBuilder<T> maxTasksInQueue(int num) {
            this.maxTasksInQueue = num;
            return this;
        }

        public AbstractBuilder<T> threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public AbstractBuilder<T> statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        public AbstractBuilder<T> traceTaskExecution(boolean enabled) {
            this.traceTaskExecution = enabled;
            return this;
        }

        public AbstractBuilder<T> traceTaskWarnTimeMicroSec(long warnTimeMicroSec) {
            this.warnTimeMicroSec = warnTimeMicroSec;
            return this;
        }

        @SuppressWarnings("unchecked")
        public T build() {
=======
    public static class SchedulerBuilder extends OrderedExecutor.AbstractBuilder<OrderedScheduler> {
        @Override
        public OrderedScheduler build() {
>>>>>>> 2346686c3b8621a585ad678926adf60206227367
            if (null == threadFactory) {
                threadFactory = new DefaultThreadFactory(name);
            }
            return new OrderedScheduler(
                name,
                numThreads,
                threadFactory,
                statsLogger,
                traceTaskExecution,
<<<<<<< HEAD
                warnTimeMicroSec,
                maxTasksInQueue);
        }

    }

    private class TimedRunnable implements SafeRunnable {
        final SafeRunnable runnable;
        final long initNanos;

        TimedRunnable(SafeRunnable runnable) {
            this.runnable = runnable;
            this.initNanos = MathUtils.nowInNano();
         }

        @Override
        public void safeRun() {
            taskPendingStats.registerSuccessfulEvent(initNanos, TimeUnit.NANOSECONDS);
            long startNanos = MathUtils.nowInNano();
            this.runnable.safeRun();
            long elapsedMicroSec = MathUtils.elapsedMicroSec(startNanos);
            taskExecutionStats.registerSuccessfulEvent(elapsedMicroSec, TimeUnit.MICROSECONDS);
            if (elapsedMicroSec >= warnTimeMicroSec) {
                LOGGER.warn("Runnable {}:{} took too long {} micros to execute.",
                            new Object[] { runnable, runnable.getClass(), elapsedMicroSec });
            }
=======
                preserveMdcForTaskExecution,
                warnTimeMicroSec,
                maxTasksInQueue);
>>>>>>> 2346686c3b8621a585ad678926adf60206227367
        }
    }

    /**
     * Constructs Safe executor.
     *
     * @param numThreads
     *            - number of threads
     * @param baseName
     *            - base name of executor threads
     * @param threadFactory
     *            - for constructing threads
     * @param statsLogger
     *            - for reporting executor stats
     * @param traceTaskExecution
     *            - should we stat task execution
     * @param preserveMdcForTaskExecution
     *            - should we preserve MDC for task execution
     * @param warnTimeMicroSec
     *            - log long task exec warning after this interval
     */
    private OrderedScheduler(String baseName,
                               int numThreads,
                               ThreadFactory threadFactory,
                               StatsLogger statsLogger,
                               boolean traceTaskExecution,
<<<<<<< HEAD
                               long warnTimeMicroSec,
                               int maxTasksInQueue) {
        checkArgument(numThreads > 0);
        checkArgument(!StringUtils.isBlank(baseName));

        this.maxTasksInQueue = maxTasksInQueue;
        this.warnTimeMicroSec = warnTimeMicroSec;
        name = baseName;
        threads = new ListeningScheduledExecutorService[numThreads];
        threadIds = new long[numThreads];
        for (int i = 0; i < numThreads; i++) {
            final ScheduledThreadPoolExecutor thread = new ScheduledThreadPoolExecutor(1,
                    new ThreadFactoryBuilder()
                        .setNameFormat(name + "-" + getClass().getSimpleName() + "-" + i + "-%d")
                        .setThreadFactory(threadFactory)
                        .build());
            threads[i] = new BoundedScheduledExecutorService(thread, this.maxTasksInQueue);

            final int idx = i;
            try {
                threads[idx].submit(new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        threadIds[idx] = Thread.currentThread().getId();
                    }
                }).get();
            } catch (InterruptedException e) {
                throw new RuntimeException("Couldn't start thread " + i, e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Couldn't start thread " + i, e);
            }

            // Register gauges
            statsLogger.registerGauge(String.format("%s-queue-%d", name, idx), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return thread.getQueue().size();
                }
            });
            statsLogger.registerGauge(String.format("%s-completed-tasks-%d", name, idx), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return thread.getCompletedTaskCount();
                }
            });
            statsLogger.registerGauge(String.format("%s-total-tasks-%d", name, idx), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return thread.getTaskCount();
                }
            });
        }

        // Stats
        this.taskExecutionStats = statsLogger.scope(name).getOpStatsLogger("task_execution");
        this.taskPendingStats = statsLogger.scope(name).getOpStatsLogger("task_queued");
        this.traceTaskExecution = traceTaskExecution;
    }

    public ListeningScheduledExecutorService chooseThread() {
        // skip random # generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[rand.nextInt(threads.length)];
    }

    public ListeningScheduledExecutorService chooseThread(Object orderingKey) {
        // skip hashcode generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[MathUtils.signSafeMod(orderingKey.hashCode(), threads.length)];
=======
                               boolean preserveMdcForTaskExecution,
                               long warnTimeMicroSec,
                               int maxTasksInQueue) {
        super(baseName, numThreads, threadFactory, statsLogger, traceTaskExecution,
                preserveMdcForTaskExecution, warnTimeMicroSec, maxTasksInQueue, false /* enableBusyWait */);
>>>>>>> 2346686c3b8621a585ad678926adf60206227367
    }

    @Override
    protected ScheduledThreadPoolExecutor createSingleThreadExecutor(ThreadFactory factory) {
        return new ScheduledThreadPoolExecutor(1, factory);
    }

    @Override
    protected ListeningScheduledExecutorService getBoundedExecutor(ThreadPoolExecutor executor) {
        return new BoundedScheduledExecutorService((ScheduledThreadPoolExecutor) executor, this.maxTasksInQueue);
    }

    @Override
    protected ListeningScheduledExecutorService addExecutorDecorators(ExecutorService executor) {
        return new OrderedSchedulerDecoratedThread((ListeningScheduledExecutorService) executor);
    }

    @Override
    public ListeningScheduledExecutorService chooseThread() {
        return (ListeningScheduledExecutorService) super.chooseThread();
    }

    @Override
    public ListeningScheduledExecutorService chooseThread(Object orderingKey) {
        return (ListeningScheduledExecutorService) super.chooseThread(orderingKey);
    }

    @Override
    public ListeningScheduledExecutorService chooseThread(long orderingKey) {
        return (ListeningScheduledExecutorService) super.chooseThread(orderingKey);
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key.
     *
     * @param orderingKey
     * @param callable
     */
    public <T> ListenableFuture<T> submitOrdered(Object orderingKey,
                                                 Callable<T> callable) {
        return chooseThread(orderingKey).submit(callable);
    }

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param command - the SafeRunnable to execute
     * @param delay - the time from now to delay execution
     * @param unit - the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose get() method
     *         will return null upon completion
     */
    public ScheduledFuture<?> schedule(SafeRunnable command, long delay, TimeUnit unit) {
        return chooseThread().schedule(timedRunnable(command), delay, unit);
    }

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param orderingKey - the key used for ordering
     * @param command - the SafeRunnable to execute
     * @param delay - the time from now to delay execution
     * @param unit - the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose get() method
     *         will return null upon completion
     */
    public ScheduledFuture<?> scheduleOrdered(Object orderingKey, SafeRunnable command, long delay, TimeUnit unit) {
        return chooseThread(orderingKey).schedule(command, delay, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period.
     *
     * <p>For more details check {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
     *
     * @param command - the SafeRunnable to execute
     * @param initialDelay - the time to delay first execution
     * @param period - the period between successive executions
     * @param unit - the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose get()
     * method will throw an exception upon cancellation
     */
    public ScheduledFuture<?> scheduleAtFixedRate(SafeRunnable command, long initialDelay, long period, TimeUnit unit) {
        return chooseThread().scheduleAtFixedRate(timedRunnable(command), initialDelay, period, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period.
     *
     * <p>For more details check {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
     *
     * @param orderingKey - the key used for ordering
     * @param command - the SafeRunnable to execute
     * @param initialDelay - the time to delay first execution
     * @param period - the period between successive executions
     * @param unit - the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose get() method
     * will throw an exception upon cancellation
     */
    public ScheduledFuture<?> scheduleAtFixedRateOrdered(Object orderingKey, SafeRunnable command, long initialDelay,
            long period, TimeUnit unit) {
        return chooseThread(orderingKey).scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently
     * with the given delay between the termination of one execution and the commencement of the next.
     *
     * <p>For more details check {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}
     * .
     *
     * @param command - the SafeRunnable to execute
     * @param initialDelay - the time to delay first execution
     * @param delay - the delay between the termination of one execution and the commencement of the next
     * @param unit - the time unit of the initialDelay and delay parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose get() method
     * will throw an exception upon cancellation
     */
    public ScheduledFuture<?> scheduleWithFixedDelay(SafeRunnable command, long initialDelay, long delay,
            TimeUnit unit) {
        return chooseThread().scheduleWithFixedDelay(timedRunnable(command), initialDelay, delay, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently
     * with the given delay between the termination of one execution and the commencement of the next.
     *
     * <p>For more details check {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}
     * .
     *
     * @param orderingKey - the key used for ordering
     * @param command - the SafeRunnable to execute
     * @param initialDelay - the time to delay first execution
     * @param delay - the delay between the termination of one execution and the commencement of the next
     * @param unit - the time unit of the initialDelay and delay parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose get() method
     * will throw an exception upon cancellation
     */
    public ScheduledFuture<?> scheduleWithFixedDelayOrdered(Object orderingKey, SafeRunnable command, long initialDelay,
            long delay, TimeUnit unit) {
        return chooseThread(orderingKey).scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }


    //
    // Methods for implementing {@link ScheduledExecutorService}
    //

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return chooseThread().schedule(timedRunnable(command), delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return chooseThread().schedule(timedCallable(callable), delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                  long initialDelay, long period, TimeUnit unit) {
        return chooseThread().scheduleAtFixedRate(timedRunnable(command), initialDelay, period, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                     long initialDelay, long delay, TimeUnit unit) {
        return chooseThread().scheduleWithFixedDelay(timedRunnable(command), initialDelay, delay, unit);
    }

    class OrderedSchedulerDecoratedThread extends ForwardingListeningExecutorService
        implements ListeningScheduledExecutorService {
        private final ListeningScheduledExecutorService delegate;

        private OrderedSchedulerDecoratedThread(ListeningScheduledExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
            protected ListeningExecutorService delegate() {
                return delegate;
            }

            @Override
            public ListenableScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
                return delegate.schedule(timedRunnable(command), delay, unit);
            }

            @Override
            public <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
                return delegate.schedule(timedCallable(callable), delay, unit);
            }

            @Override
            public ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                                    long initialDelay, long period, TimeUnit unit) {
                return delegate.scheduleAtFixedRate(timedRunnable(command), initialDelay, period, unit);
            }

            @Override
            public ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                                       long initialDelay, long delay, TimeUnit unit) {
                return delegate.scheduleAtFixedRate(timedRunnable(command), initialDelay, delay, unit);
            }

            @Override
            public <T> ListenableFuture<T> submit(Callable<T> task) {
                return super.submit(timedCallable(task));
            }

            @Override
            public ListenableFuture<?> submit(Runnable task) {
                return super.submit(timedRunnable(task));
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
                return super.invokeAll(timedCallables(tasks));
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                                 long timeout, TimeUnit unit) throws InterruptedException {
                return super.invokeAll(timedCallables(tasks), timeout, unit);
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                    throws InterruptedException, ExecutionException {
                return super.invokeAny(timedCallables(tasks));
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
                                   TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

                return super.invokeAny(timedCallables(tasks), timeout, unit);
            }

            @Override
            public <T> ListenableFuture<T> submit(Runnable task, T result) {
                return super.submit(timedRunnable(task), result);
            }

            @Override
            public void execute(Runnable command) {
                super.execute(timedRunnable(command));
            }
        }

}
