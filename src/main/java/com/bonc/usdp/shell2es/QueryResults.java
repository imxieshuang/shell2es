package com.bonc.usdp.shell2es;

import com.google.common.base.Throwables;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Created by Administrator on 2017/8/22.
 */
public class QueryResults {
    private static final Signal SIGINT = new Signal("INT");
    private final AtomicBoolean ignoreUserInterrupt = new AtomicBoolean();
    private final AtomicBoolean userAbortedQuery = new AtomicBoolean();
    private Iterable<List<Object>> data;
    private List<String> columns;
    private QueryRunner queryRunner;

    public QueryResults(Iterable<List<Object>> data, List<String> columns, QueryRunner queryRunner) {
        this.data = requireNonNull(data, "data is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
    }

    public void renderOutput(PrintStream out, ClientOptions.OutputFormat outputFormat, boolean interactive) {
        Thread clientThread = Thread.currentThread();
        SignalHandler oldHandler = Signal.handle(SIGINT, signal -> {
            if (ignoreUserInterrupt.get()) {
                return;
            }
            userAbortedQuery.set(true);
            clientThread.interrupt();
        });
        try {
            renderQueryOutput(out, outputFormat, interactive);
        } finally {
            Signal.handle(SIGINT, oldHandler);
            Thread.interrupted(); // clear interrupt status
        }
    }

    private void renderQueryOutput(PrintStream out, ClientOptions.OutputFormat outputFormat, boolean interactive) {
        @SuppressWarnings("resource")
        PrintStream errorChannel = interactive ? out : System.err;
        if (columns == null||data==null) {
            errorChannel.printf("Query %s has no columns\n");
            return;
        }
        if (!queryRunner.isClosed()) {
            renderResults(out, outputFormat, interactive, this.columns);
        }
    }

    private void renderResults(PrintStream out, ClientOptions.OutputFormat outputFormat, boolean interactive, List<String> fieldNames)
    {
        try {
            doRenderResults(out, outputFormat, interactive, fieldNames);
        }
        catch (QueryAbortedException e) {
            System.out.println("(query aborted by user)");
            try {
                queryRunner.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void doRenderResults(PrintStream out, ClientOptions.OutputFormat format, boolean interactive, List<String> fieldNames)
            throws IOException
    {
        if (interactive) {
            pageOutput(format, columns);
        }
        else {
            sendOutput(out, format, fieldNames);
        }
    }

    private void sendOutput(PrintStream out, ClientOptions.OutputFormat format, List<String> fieldNames)
            throws IOException
    {
        try (OutputHandler handler = createOutputHandler(format, createWriter(out), fieldNames)) {
            handler.processRows(this);
        }
    }

    private void pageOutput(ClientOptions.OutputFormat format, List<String> fieldNames)
            throws IOException
    {
        try (Pager pager = Pager.create();
             ThreadInterruptor clientThread = new ThreadInterruptor();
             Writer writer = createWriter(pager);
             OutputHandler handler = createOutputHandler(format, writer, fieldNames)) {
            if (!pager.isNullPager()) {
                // ignore the user pressing ctrl-C while in the pager
                ignoreUserInterrupt.set(true);
                pager.getFinishFuture().thenRun(() -> {
                    userAbortedQuery.set(true);
                    ignoreUserInterrupt.set(false);
                    clientThread.interrupt();
                });
            }
            handler.processRows(this);
        }
        catch (RuntimeException | IOException e) {
            // clear interrupt flag before throwing an exception
            Thread.interrupted();
            if (userAbortedQuery.get() && !(e instanceof QueryAbortedException)) {
                throw new QueryAbortedException(e);
            }
            throw e;
        }
    }

    private static OutputHandler createOutputHandler(ClientOptions.OutputFormat format, Writer writer, List<String> fieldNames)
    {
        return new OutputHandler(createOutputPrinter(format, writer, fieldNames));
    }

    private static Writer createWriter(OutputStream out)
    {
        return new OutputStreamWriter(out, UTF_8);
    }

    private static class ThreadInterruptor
            implements Closeable
    {
        private final Thread thread = Thread.currentThread();
        private final AtomicBoolean processing = new AtomicBoolean(true);

        public synchronized void interrupt()
        {
            if (processing.get()) {
                thread.interrupt();
            }
        }

        @Override
        public synchronized void close()
        {
            processing.set(false);
        }
    }

    public Iterable<List<Object>> getData() {
        return data;
    }

    public List<String> getColumns() {
        return columns;
    }

    private static OutputPrinter createOutputPrinter(ClientOptions.OutputFormat format, Writer writer, List<String> fieldNames)
    {
        switch (format) {
            case ALIGNED:
                return new AlignedTablePrinter(fieldNames, writer);
            case VERTICAL:
                return new VerticalRecordPrinter(fieldNames, writer);
            case CSV:
                return new CsvPrinter(fieldNames, writer, false);
            case CSV_HEADER:
                return new CsvPrinter(fieldNames, writer, true);
            case TSV:
                return new TsvPrinter(fieldNames, writer, false);
            case TSV_HEADER:
                return new TsvPrinter(fieldNames, writer, true);
            case NULL:
                return new NullPrinter();
        }
        throw new RuntimeException(format + " not supported");
    }
}
