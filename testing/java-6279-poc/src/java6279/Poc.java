/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package java6279;

import java.io.File;
import java.lang.ref.Cleaner;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JAVA-6279 proof of concept: does a non-terminated driver thread pin the class loader that loaded the driver?
 *
 * <p>This is a portable rework of the experiment in
 * <a href="https://github.com/stIncMale/mongo-java-driver/commit/862b7d75fa0629e2b7c9cc4d6e8761b1678934dd">
 * Valentin Kovalenko's {@code primer} commit</a>. That version hardcoded an absolute path to one developer's
 * {@code build/classes} directory; here the class file directory and the driver classpath are supplied as system
 * properties by {@code run.sh}, so it runs anywhere.</p>
 *
 * <p>Two groups of scenarios:</p>
 * <ul>
 *     <li>{@code primer} — synthetic classes in a throwaway child class loader, reproducing the original finding: a
 *     live thread started from a class's static initializer keeps that class and its whole class loader strongly
 *     reachable, even though the thread's {@code Runnable} is defined by a <em>parent</em>-loaded class and so
 *     references nothing in the child loader.</li>
 *     <li>{@code driver} — the real case behind JAVA-6279: load the driver into a child class loader, touch (or open
 *     and close) it, then check whether that loader can be collected while {@code BufferPoolPruner} is alive.</li>
 * </ul>
 *
 * <p>Every scenario reports {@code COLLECTED} or {@code PINNED}; nothing is asserted, because "the GC did not get
 * around to it" and "something holds a strong reference" are not distinguishable in the general case. The bounded
 * wait plus the explicit {@link System#gc()} nudges make {@code PINNED} strong evidence in practice, and the control
 * scenarios establish that the harness can observe a collection at all.</p>
 *
 * <p>Not wired into Gradle: it needs {@link System#gc()}, custom class loaders and multi-second GC windows, none of
 * which belong in the normal test suite.</p>
 */
public final class Poc {
    /**
     * How long the threads started by the primer classes sleep. Must comfortably outlive {@link #GC_WINDOW} so that
     * a {@code PINNED} verdict is attributable to a live thread rather than to one that already finished.
     */
    private static final Duration THREAD_LIFETIME = Duration.ofSeconds(30);

    /**
     * How long we nudge the GC before declaring a referent unreachable-or-not. Override with
     * {@code -Djava6279.gcWindowSeconds=...} when a fix under test terminates its thread on a timer longer than the
     * default — e.g. verifying a {@code CommonExecutor} keep-alive of 30s needs a window comfortably beyond it.
     */
    private static final Duration GC_WINDOW =
            Duration.ofSeconds(Long.getLong("java6279.gcWindowSeconds", 10L));

    /**
     * The driver scenarios need a longer window than the primer ones. With JAVA-6279 fixed the pruner stops on a timer
     * derived from {@code maxIdleTime} (one minute by default) and the thread then times out after the keep-alive, so
     * the loader is released roughly 90s after the last buffer release rather than immediately. Override with
     * {@code -Djava6279.driverGcWindowSeconds=...}.
     */
    private static final Duration DRIVER_GC_WINDOW =
            Duration.ofSeconds(Long.getLong("java6279.driverGcWindowSeconds", 150L));

    private static final String PRIMER_PACKAGE = "java6279.primer.";

    /**
     * The body of every thread the primer classes start. Deliberately defined <em>here</em>, in a class the child
     * loader delegates to its parent, so the running thread's {@code Runnable} has no reference of any kind into the
     * child loader. That is the whole point of the experiment.
     */
    public static final Runnable SLEEPING_RUNNABLE = new Runnable() {
        @Override
        public void run() {
            try {
                log("thread %s is sleeping for %s", Thread.currentThread().getName(), THREAD_LIFETIME);
                Thread.sleep(THREAD_LIFETIME.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                log("thread %s terminated", Thread.currentThread().getName());
            }
        }
    };

    /**
     * A hook thread both constructed and bodied by this parent-loaded class, before any child loader exists. The
     * child class only calls {@code addShutdownHook} on it. This is the only shutdown-hook shape that does not pin —
     * and, being unable to reference driver code, the only one that cannot stop the pruner.
     */
    public static final Thread PARENT_BUILT_HOOK_THREAD = new Thread(new Runnable() {
        @Override
        public void run() {
            log("the fully parent-built shutdown hook ran");
        }
    }, "java6279-shutdown-hook-parent-built");

    private Poc() {
    }

    public static void main(final String... args) throws Exception {
        log("%s %s by %s", System.getProperty("java.vm.name"), System.getProperty("java.version"),
                System.getProperty("java.vendor"));
        String what = args.length > 0 ? args[0] : "all";
        List<Result> results = new ArrayList<>();
        if (what.equals("primer") || what.equals("all")) {
            results.addAll(primerScenarios());
        }
        if (what.equals("driver") || what.equals("all")) {
            results.addAll(driverScenarios());
        }
        report(results);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // primer scenarios
    // ---------------------------------------------------------------------------------------------------------------

    private static List<Result> primerScenarios() throws Exception {
        Path classesDir = Paths.get(requireProperty("java6279.classesDir"));
        List<Result> results = new ArrayList<>();
        results.add(primerScenario(classesDir, "Inert",
                "control: a child-loaded class that starts no thread", Expectation.COLLECTED));
        results.add(primerScenario(classesDir, "StartsOwnThread",
                "a child-loaded class that constructs and starts a thread in its static initializer",
                Expectation.PINNED));
        results.add(primerScenario(classesDir, "StartsOwnThreadNettyStyle",
                "as above, but with Netty's GlobalEventExecutor mitigation: null the context class loader around "
                        + "thread creation (netty#7290, JDK-7008595)",
                Expectation.PINNED));
        results.add(primerScenario(classesDir, "StartsParentBuiltThread",
                "a child-loaded class that starts a Thread object constructed by a parent-loaded class",
                Expectation.UNKNOWN));
        results.add(primerScenario(classesDir, "InheritsContextClassLoader",
                "a parent-built thread that INHERITS the child loader as its context class loader -- the leak edge "
                        + "Netty's mitigation targets, i.e. a driver thread pinning an application's loader",
                Expectation.PINNED));
        results.add(primerScenario(classesDir, "InheritsContextClassLoaderButNulled",
                "the same, context class loader nulled after construction. CONFOUNDED: this class's <clinit> is on "
                        + "the stack, and the stack capture pins regardless -- see cclOnly/* for the clean isolation",
                Expectation.PINNED));
        results.add(primerScenario(classesDir, "InheritsContextClassLoaderNettyDance",
                "the same, nulling the CALLING thread's context class loader before construction as Netty does. "
                        + "Also CONFOUNDED by the stack frame: the stack capture cannot be nulled away",
                Expectation.PINNED));
        // Isolate the context class loader edge with NO child-loaded class on the stack at construction time. The
        // scenarios above cannot do this: a primer class's static initializer is necessarily on the stack, and the
        // stack capture dominates, masking whatever the context class loader does.
        results.add(contextClassLoaderOnlyScenario(classesDir, false));
        results.add(contextClassLoaderOnlyScenario(classesDir, true));
        results.add(primerScenario(classesDir, "RegistersShutdownHook",
                "the shutdown-hook alternative: register a hook to stop the pruner, start no thread. "
                        + "ApplicationShutdownHooks holds hooks in a static map until JVM exit",
                Expectation.PINNED));
        results.add(primerScenario(classesDir, "RegistersShutdownHookNettyStyle",
                "shutdown hook + Netty's context class loader nulling, hook body still child-loaded",
                Expectation.PINNED));
        results.add(primerScenario(classesDir, "RegistersShutdownHookParentBody",
                "shutdown hook + nulled context class loader + parent-loaded hook body -- collectable, but the hook "
                        + "cannot reference driver code, so it cannot stop the pruner",
                Expectation.COLLECTED));
        results.add(primerScenario(classesDir, "StaticSingletonExecutor",
                "models CommonExecutor: a static singleton whose executor is never shut down, with the Cleaner its "
                        + "VAKOTODO proposes -- see whether the cleaning action can ever run",
                Expectation.PINNED));
        return results;
    }

    /**
     * The clean isolation of the context class loader edge. Everything happens in this parent-loaded class: a child
     * class is initialized and returned from, then the thread is constructed with only {@code Poc} frames on the stack
     * while the calling thread's context class loader is the child loader. So the only possible edge into that loader
     * is the context class loader, and {@code nullContextClassLoader} decides whether it exists.
     *
     * <p>This is Netty's configuration, and the one the {@code DaemonThreadFactory} change is aimed at: a driver thread
     * created on behalf of an application, pinning the <em>application's</em> loader.</p>
     */
    private static Result contextClassLoaderOnlyScenario(final Path classesDir, final boolean nullContextClassLoader)
            throws Exception {
        String name = "cclOnly/" + (nullContextClassLoader ? "nulled" : "inherited");
        banner(name);
        PhantomReachableWatch watch = loadAndForgetCclOnly(classesDir, nullContextClassLoader);
        boolean collected = watch.awaitCollected(GC_WINDOW);
        return new Result(name,
                nullContextClassLoader
                        ? "thread constructed with NO child frame on the stack and the context class loader nulled"
                        : "thread constructed with NO child frame on the stack, inheriting the child loader as its "
                                + "context class loader",
                nullContextClassLoader ? Expectation.COLLECTED : Expectation.PINNED, collected, watch.elapsed());
    }

    private static PhantomReachableWatch loadAndForgetCclOnly(final Path classesDir,
            final boolean nullContextClassLoader) throws Exception {
        ClassLoader loader = new PrimerClassLoader(classesDir);
        Class.forName(PRIMER_PACKAGE + "Inert", true, loader);
        Thread callingThread = Thread.currentThread();
        ClassLoader previous = callingThread.getContextClassLoader();
        callingThread.setContextClassLoader(loader);
        try {
            Thread thread = new Thread(SLEEPING_RUNNABLE,
                    "java6279-cclOnly-" + (nullContextClassLoader ? "nulled" : "inherited"));
            log("built %s with inherited context class loader %s", thread.getName(), thread.getContextClassLoader());
            if (nullContextClassLoader) {
                thread.setContextClassLoader(null);
            }
            thread.start();
        } finally {
            callingThread.setContextClassLoader(previous);
        }
        return new PhantomReachableWatch(loader, loader.toString());
    }

    /**
     * Loads {@code java6279.primer.<simpleName>} in a fresh child loader, forgets every strong reference to the
     * loader and to the classes it defined, and reports whether the loader became phantom reachable.
     */
    private static Result primerScenario(final Path classesDir, final String simpleName, final String description,
            final Expectation expectation) throws Exception {
        banner("primer/" + simpleName);
        // Loading the trigger class and dropping the references happens in a separate frame so that no local variable
        // in this frame keeps the loader alive while we wait for the GC.
        CLEANER_RAN.set(false);
        PhantomReachableWatch watch = loadAndForgetPrimer(classesDir, simpleName);
        boolean collected = watch.awaitCollected(GC_WINDOW);
        if (simpleName.equals("StaticSingletonExecutor")) {
            log("cleaning action ran: %s  (the CommonExecutor VAKOTODO proposes relying on this)", CLEANER_RAN.get());
        }
        return new Result("primer/" + simpleName, description, expectation, collected, watch.elapsed());
    }

    private static PhantomReachableWatch loadAndForgetPrimer(final Path classesDir, final String simpleName)
            throws Exception {
        ClassLoader loader = new PrimerClassLoader(classesDir);
        // Initialize the trigger class, then also load an inert sibling. The sibling is what the original experiment
        // used to show the pinning is loader-wide and not specific to the class that started the thread.
        Class.forName(PRIMER_PACKAGE + simpleName, true, loader);
        Class.forName(PRIMER_PACKAGE + "Inert", true, loader);
        return new PhantomReachableWatch(loader, loader.toString());
    }

    /**
     * A {@code Thread} object constructed while this class is initialized -- that is, before any primer class loader
     * exists. {@code java6279.primer.StartsParentBuiltThread} only calls {@code start()} on it. Being a field rather
     * than the result of a factory method matters: if the object were constructed on demand, a child-loaded class
     * would be on the stack at construction time and the distinction the scenario is drawing would be lost.
     *
     * <p>{@code App.THREAD} in the original experiment.</p>
     */
    public static final Thread PARENT_BUILT_THREAD = new Thread(SLEEPING_RUNNABLE, "java6279-parent-built");

    /**
     * Builds and starts a thread from <em>this</em> parent-loaded class, so there is no construction-site pin, and the
     * only possible edge into a child loader is the inherited context class loader. With {@code nullContextClassLoader}
     * this applies the mitigation added to {@code DaemonThreadFactory}; without it, the thread keeps whatever the
     * calling thread's context class loader was.
     */
    public static void buildAndStartThreadInheritingCcl(final boolean nullContextClassLoader) {
        Thread thread = new Thread(SLEEPING_RUNNABLE,
                "java6279-ccl-" + (nullContextClassLoader ? "nulled" : "inherited"));
        log("built %s with inherited context class loader %s", thread.getName(), thread.getContextClassLoader());
        if (nullContextClassLoader) {
            thread.setContextClassLoader(null);
        }
        thread.start();
    }

    /**
     * Netty's full mitigation: null the <em>calling</em> thread's context class loader BEFORE constructing, then
     * restore it. The distinction from {@link #buildAndStartThreadInheritingCcl} matters — construction captures the
     * creating thread's context, so nulling the new thread's field afterwards is too late.
     */
    public static void buildAndStartThreadNettyDance() {
        Thread callingThread = Thread.currentThread();
        ClassLoader parentCcl = callingThread.getContextClassLoader();
        callingThread.setContextClassLoader(null);
        try {
            Thread thread = new Thread(SLEEPING_RUNNABLE, "java6279-ccl-netty-dance");
            thread.setContextClassLoader(null);
            log("built %s with context class loader %s", thread.getName(), thread.getContextClassLoader());
            thread.start();
        } finally {
            callingThread.setContextClassLoader(parentCcl);
        }
    }

    public static void log(final String format, final Object... args) {
        System.err.printf("[java6279] " + format + "%n", args);
    }

    /**
     * Registers a cleaning action, as {@code CommonExecutor}'s {@code VAKOTODO} proposes doing to shut its executor
     * down. The {@link Cleaner} lives here, in a parent-loaded class, so that its own thread is not itself a reason
     * for a child loader to be pinned.
     */
    public static void registerCleaner(final Object referent, final Runnable action) {
        CLEANER.register(referent, () -> {
            CLEANER_RAN.set(true);
            action.run();
        });
    }

    private static final Cleaner CLEANER = Cleaner.create();

    /** Whether any cleaning action registered via {@link #registerCleaner} has run. */
    private static final AtomicBoolean CLEANER_RAN = new AtomicBoolean();

    // ---------------------------------------------------------------------------------------------------------------
    // driver scenarios
    // ---------------------------------------------------------------------------------------------------------------

    private static List<Result> driverScenarios() throws Exception {
        URL[] driverClasspath = driverClasspath();
        List<Result> results = new ArrayList<>();
        results.add(driverScenario(driverClasspath, DriverAction.LOAD_ONLY,
                "control: driver classes loaded but PowerOfTwoBufferPool.DEFAULT never initialized",
                Expectation.COLLECTED));
        results.add(driverScenario(driverClasspath, DriverAction.TOUCH_DEFAULT_POOL,
                "PowerOfTwoBufferPool.DEFAULT initialized. Before JAVA-6279 this alone started the pruner and "
                        + "pinned the loader; an empty pool must now start no thread", Expectation.COLLECTED));
        results.add(driverScenario(driverClasspath, DriverAction.TOUCH_DEFAULT_POOL_THEN_DISABLE_PRUNING,
                "the same, then disablePruning() called reflectively -- the workaround from GitHub issue 2029",
                Expectation.COLLECTED));
        results.add(driverScenario(driverClasspath, DriverAction.OPEN_AND_CLOSE_CLIENT,
                "MongoClients.create(...) followed by close() -- the symptom as reported. With JAVA-6279 fixed the "
                        + "pruner drains the pool, stops, and its thread times out, so the loader is released",
                Expectation.COLLECTED));
        results.add(driverScenario(driverClasspath, DriverAction.OPEN_AND_CLOSE_CLIENT_THEN_DISABLE_PRUNING,
                "the same, plus disablePruning() -- shows the pruner is the only remaining pin after close()",
                Expectation.COLLECTED));
        results.add(driverScenario(driverClasspath,
                DriverAction.OPEN_AND_CLOSE_CLIENT_THEN_DISABLE_PRUNING_AND_TOUCH_COMMON_EXECUTOR,
                "the same, plus starting CommonExecutor's thread as an async retry backoff would -- backpressure "
                        + "branch only, isolates CommonExecutor as a second independent pin",
                Expectation.UNKNOWN));
        return results;
    }

    private static Result driverScenario(final URL[] driverClasspath, final DriverAction action,
            final String description, final Expectation expectation) throws Exception {
        banner("driver/" + action);
        PhantomReachableWatch watch = loadAndForgetDriver(driverClasspath, action);
        boolean collected = watch.awaitCollected(DRIVER_GC_WINDOW);
        if (!collected) {
            log("live non-JVM threads after the GC window: %s", nonJvmThreadNames());
        }
        if (action.skipped) {
            return new Result("driver/" + action, "SKIPPED (not applicable to this branch) -- " + description,
                    Expectation.UNKNOWN, collected, watch.elapsed());
        }
        return new Result("driver/" + action, description, expectation, collected, watch.elapsed());
    }

    private static PhantomReachableWatch loadAndForgetDriver(final URL[] driverClasspath, final DriverAction action)
            throws Exception {
        // Parent is the platform class loader, not the application one, so the driver classes on this classpath are
        // genuinely defined by the child loader -- exactly the situation of an application server or OSGi container
        // loading the driver as part of a redeployable unit.
        URLClassLoader loader = new URLClassLoader("java6279-driver", driverClasspath,
                ClassLoader.getPlatformClassLoader());
        action.run(loader);
        // The loader is deliberately *not* closed. close() only releases jar file handles, it does not affect
        // reachability, and doing it here would make any still-running driver thread fail with NoClassDefFoundError
        // and cloud the result.
        return new PhantomReachableWatch(loader, loader.toString());
    }

    private enum DriverAction {
        LOAD_ONLY {
            @Override
            void run(final ClassLoader loader) throws Exception {
                Class.forName("com.mongodb.internal.connection.PowerOfTwoBufferPool", false, loader);
                log("loaded PowerOfTwoBufferPool without initializing it");
            }
        },
        TOUCH_DEFAULT_POOL {
            @Override
            void run(final ClassLoader loader) throws Exception {
                defaultPool(loader);
            }
        },
        TOUCH_DEFAULT_POOL_THEN_DISABLE_PRUNING {
            @Override
            void run(final ClassLoader loader) throws Exception {
                disablePruning(defaultPool(loader));
            }
        },
        OPEN_AND_CLOSE_CLIENT {
            @Override
            void run(final ClassLoader loader) throws Exception {
                openAndCloseClient(loader);
            }
        },
        OPEN_AND_CLOSE_CLIENT_THEN_DISABLE_PRUNING {
            @Override
            void run(final ClassLoader loader) throws Exception {
                openAndCloseClient(loader);
                disablePruning(defaultPool(loader));
            }
        },

        /**
         * Only meaningful on the backpressure branch, where {@code CommonExecutor} exists. Starts its
         * {@code CommonScheduler} thread the way an async retry backoff would, then closes the client and disables
         * pruning -- so a PINNED result isolates {@code CommonExecutor} as a second, independent pin.
         */
        OPEN_AND_CLOSE_CLIENT_THEN_DISABLE_PRUNING_AND_TOUCH_COMMON_EXECUTOR {
            @Override
            void run(final ClassLoader loader) throws Exception {
                Class<?> commonExecutorClass;
                try {
                    commonExecutorClass = Class.forName("com.mongodb.internal.thread.CommonExecutor", true, loader);
                } catch (ClassNotFoundException e) {
                    log("CommonExecutor is not on this branch -- scenario skipped");
                    skipped = true;
                    return;
                }
                openAndCloseClient(loader);
                disablePruning(defaultPool(loader));
                Object commonExecutor = commonExecutorClass.getMethod("commonExecutor").invoke(null);
                // `schedule` is package private, as `disablePruning` is.
                java.lang.reflect.Method schedule = commonExecutorClass.getDeclaredMethod(
                        "schedule", Runnable.class, java.time.Duration.class, java.util.concurrent.Executor.class);
                schedule.setAccessible(true);
                java.util.concurrent.Executor direct = Runnable::run;
                schedule.invoke(commonExecutor, (Runnable) () -> { }, java.time.Duration.ofMillis(1), direct);
                log("scheduled on CommonExecutor; live non-JVM threads: %s", nonJvmThreadNames());
            }
        };

        /** Set when a scenario cannot apply to the branch under test. */
        boolean skipped;

        abstract void run(ClassLoader loader) throws Exception;

        static Object defaultPool(final ClassLoader loader) throws Exception {
            Class<?> poolClass = Class.forName("com.mongodb.internal.connection.PowerOfTwoBufferPool", true, loader);
            Object pool = poolClass.getField("DEFAULT").get(null);
            log("initialized PowerOfTwoBufferPool.DEFAULT; live non-JVM threads: %s", nonJvmThreadNames());
            return pool;
        }

        /** {@code disablePruning} is package private, hence the reflection. This is the workaround from issue 2029. */
        static void disablePruning(final Object pool) throws Exception {
            java.lang.reflect.Method disablePruning = pool.getClass().getDeclaredMethod("disablePruning");
            disablePruning.setAccessible(true);
            disablePruning.invoke(pool);
            log("called PowerOfTwoBufferPool.disablePruning() reflectively");
        }

        static void openAndCloseClient(final ClassLoader loader) throws Exception {
            Class<?> mongoClients = Class.forName("com.mongodb.client.MongoClients", true, loader);
            Object client = mongoClients.getMethod("create", String.class)
                    .invoke(null, System.getProperty("org.mongodb.test.uri", "mongodb://localhost:27017"));
            log("created %s", client.getClass().getName());
            // No operation is issued, so no server is needed: the point is the client's own background threads. A
            // failed heartbeat against an absent server is expected and harmless here.
            ((AutoCloseable) client).close();
            log("closed the MongoClient; live non-JVM threads: %s", nonJvmThreadNames());
        }
    }

    private static URL[] driverClasspath() throws MalformedURLException {
        String raw = requireProperty("java6279.driverCp");
        List<URL> urls = new ArrayList<>();
        for (String entry : raw.split(File.pathSeparator)) {
            if (!entry.isEmpty()) {
                urls.add(Paths.get(entry).toUri().toURL());
            }
        }
        return urls.toArray(new URL[0]);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // reachability plumbing
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * The child class loader for the primer scenarios. Defines only {@code java6279.primer.*} itself and delegates
     * everything else -- including {@link Poc} -- to its parent, so the primer classes can call back into
     * parent-loaded code without that code becoming child-loaded.
     */
    private static final class PrimerClassLoader extends ClassLoader {
        private static int instanceCount;
        private final Path classesDir;
        private final String name;

        PrimerClassLoader(final Path classesDir) {
            super(Poc.class.getClassLoader());
            this.classesDir = classesDir;
            this.name = "java6279-primer-" + (++instanceCount);
        }

        @Override
        protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> result = findLoadedClass(name);
                if (result == null) {
                    result = name.startsWith(PRIMER_PACKAGE) ? findClass(name) : getParent().loadClass(name);
                }
                if (resolve) {
                    resolveClass(result);
                }
                return result;
            }
        }

        @Override
        protected Class<?> findClass(final String name) throws ClassNotFoundException {
            if (!name.startsWith(PRIMER_PACKAGE)) {
                throw new ClassNotFoundException(name);
            }
            byte[] classFile;
            try {
                classFile = Files.readAllBytes(classesDir.resolve(name.replace('.', '/') + ".class"));
            } catch (IOException e) {
                throw new ClassNotFoundException(name, e);
            }
            return defineClass(name, classFile, 0, classFile.length);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Watches one referent and reports whether it becomes phantom reachable within a bounded window, nudging the
     * collector as it goes. Holds no strong reference to the referent.
     */
    private static final class PhantomReachableWatch {
        private final java.lang.ref.PhantomReference<Object> reference;
        private final java.lang.ref.ReferenceQueue<Object> queue = new java.lang.ref.ReferenceQueue<>();
        private final String description;
        private final long startNanos = System.nanoTime();
        private Duration elapsed = Duration.ZERO;

        PhantomReachableWatch(final Object referent, final String description) {
            this.description = description;
            this.reference = new java.lang.ref.PhantomReference<>(referent, queue);
        }

        boolean awaitCollected(final Duration window) throws InterruptedException {
            long deadlineNanos = startNanos + window.toNanos();
            while (System.nanoTime() < deadlineNanos) {
                System.gc();
                if (queue.remove(100) != null) {
                    elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
                    log("%s became phantom reachable in %s", description, elapsed);
                    reference.clear();
                    return true;
                }
            }
            elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
            log("%s is still strongly reachable after %s", description, elapsed);
            return false;
        }

        Duration elapsed() {
            return elapsed;
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // reporting
    // ---------------------------------------------------------------------------------------------------------------

    private enum Expectation {
        COLLECTED, PINNED, UNKNOWN
    }

    private static final class Result {
        private final String name;
        private final String description;
        private final Expectation expectation;
        private final boolean collected;
        private final Duration elapsed;

        Result(final String name, final String description, final Expectation expectation, final boolean collected,
                final Duration elapsed) {
            this.name = name;
            this.description = description;
            this.expectation = expectation;
            this.collected = collected;
            this.elapsed = elapsed;
        }

        String observed() {
            return collected ? "COLLECTED" : "PINNED";
        }

        boolean asExpected() {
            return expectation == Expectation.UNKNOWN
                    || (expectation == Expectation.COLLECTED) == collected;
        }
    }

    private static void report(final List<Result> results) {
        System.err.println();
        System.err.println("================================ results ================================");
        System.err.printf("%-9s %-9s %-50s %s%n", "OBSERVED", "EXPECTED", "SCENARIO", "AFTER");
        boolean allAsExpected = true;
        for (Result result : results) {
            allAsExpected &= result.asExpected();
            System.err.printf("%-9s %-9s %-50s %s%n", result.observed(), result.expectation, result.name,
                    result.elapsed);
            System.err.printf("%33s%s%n", "", result.description);
        }
        System.err.println("=========================================================================");
        System.err.println(allAsExpected
                ? "every scenario matched its expectation"
                : "AT LEAST ONE SCENARIO DID NOT MATCH ITS EXPECTATION -- see the table above");
        // The exit status reflects only whether the harness observed what it expected, so run.sh can be used in a
        // pipeline. A PINNED result for the pruner scenarios is the bug, and is the *expected* outcome today.
        if (!allAsExpected) {
            System.exit(1);
        }
    }

    private static Set<String> nonJvmThreadNames() {
        Set<String> names = new TreeSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            ThreadGroup group = thread.getThreadGroup();
            if (group != null && !"system".equals(group.getName()) && !thread.getName().equals("main")) {
                names.add(thread.getName());
            }
        }
        return names.isEmpty() ? Collections.unmodifiableSet(new TreeSet<>(Arrays.asList("(none)"))) : names;
    }

    private static String requireProperty(final String name) {
        String value = System.getProperty(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("system property " + name + " must be set; use run.sh");
        }
        return value;
    }

    private static void banner(final String scenario) {
        System.err.println();
        System.err.println("---------------- " + scenario + " ----------------");
    }
}
