package com.bonc.usdp.shell2es;

import com.bonc.usdp.shell2es.ClientOptions.OutputFormat;
import com.google.common.base.Strings;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.MemoryHistory;
import jline.internal.Configuration;
import org.fusesource.jansi.AnsiConsole;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import javax.inject.Inject;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by Administrator on 2017/8/21.
 */
@Command(name = "usou")
public class Console {
    @Inject
    public HelpOption helpOption;
    @Inject
    public ClientOptions clientOptions = new ClientOptions();
    private static Logger logger = Logger.get(Console.class);
    private static final String PROMPT_NAME = "usou";
    private static final Duration EXIT_DELAY = new Duration(3, SECONDS);

    private static final Pattern HISTORY_INDEX_PATTERN = Pattern.compile("!\\d+");

    public void run() {
        AnsiConsole.systemInstall();

        AtomicBoolean exiting = new AtomicBoolean();
        interruptThreadOnExit(Thread.currentThread(), exiting);

        QueryRunner queryRunner = new QueryRunner(clientOptions.host
                , clientOptions.port, clientOptions.index, clientOptions.clusterName);

        runConsole(queryRunner,exiting);
    }

    private static void interruptThreadOnExit(Thread thread, AtomicBoolean exiting) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            exiting.set(true);
            thread.interrupt();
            try {
                thread.join(EXIT_DELAY.toMillis());
            } catch (InterruptedException ignored) {
            }
        }));
    }

    public void runConsole(QueryRunner queryRunner, AtomicBoolean exiting) {
        try (TableNameCompleter tableNameCompleter = new TableNameCompleter(queryRunner)) {
            LineReader reader = new LineReader(getHistory(), Completion.commandCompleter()
                    , Completion.lowerCaseCommandComleter(), tableNameCompleter);
            tableNameCompleter.populateCache();
            StringBuilder buffer = new StringBuilder();
            while (!exiting.get()) {
                String prompt = PROMPT_NAME;
                if (queryRunner.getIndex() != null) {
                    prompt += ":" + queryRunner.getIndex();
                }
                if (buffer.length() > 0) {
                    prompt = Strings.repeat(" ", prompt.length() - 1) + "-";
                }
                String commandPrompt = prompt + ">";
                String line = reader.readLine(commandPrompt);

                // add buffer to history and clear on user interrupt
                if (reader.interrupted()) {
                    reader.getHistory().add(buffer.toString());
                    buffer = new StringBuilder();
                    continue;
                }

                // exit on EOF
                if (line == null) {
                    System.out.println();
                    return;
                }

                // check for special commands if this is the first line
                if (buffer.length() == 0) {
                    String command = line.trim();
                    if (HISTORY_INDEX_PATTERN.matcher(command).matches()) {
                        int historyIndex = Integer.parseInt(command.substring(1));
                        History history = reader.getHistory();
                        if ((historyIndex <= 0) || (historyIndex > history.index())) {
                            System.err.println("Command does not exist");
                            continue;
                        }
                        line = history.get(historyIndex - 1).toString();
                        System.out.println(commandPrompt + line);
                    }
                    if (command.endsWith(";")) {
                        command = command.substring(0, command.length() - 1).trim();
                    }

                    switch (command.toLowerCase(Locale.ENGLISH)) {
                        case "exit":
                        case "quit":
                            return;
                        case "history":
                            for (History.Entry entry : reader.getHistory()) {
                                System.out.printf("%5d  %s%n", entry.index() + 1, entry.value());
                            }
                            continue;
                        case "help":
                            System.out.println();
                            System.out.println(Help.getHelpText());
                            continue;
                    }
                }

                // not a command, add line to buffer
                buffer.append(line).append("\n");

                //execute any complete statements
                String sql = buffer.toString();
                OutputFormat outputFormat = OutputFormat.ALIGNED;

                process(queryRunner, sql.substring(0, sql.length() - 1), outputFormat, true);
                reader.getHistory().add(sql);

                buffer = new StringBuilder();


            }
        } catch (IOException e) {
            System.err.println("Readline error:" + e.getMessage());

        } finally {
            try {
                queryRunner.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void process(QueryRunner queryRunner, String sql, OutputFormat outputFormat, boolean interactive) {
        QueryResults results = queryRunner.run(sql);
        results.renderOutput(System.out,outputFormat,interactive);
    }


    private static MemoryHistory getHistory() {
        MemoryHistory history;
        File historyFile = new File(Configuration.getUserHome(), ".usou_history");
        try {
            history = new FileHistory(historyFile);
            history.setMaxSize(10000);
        } catch (IOException e) {
            System.err.printf("WARNING: Failed to load history file (%s): %s. " +
                            "History will not be available during this session.%n",
                    historyFile, e.getMessage());
            history = new MemoryHistory();
        }
        history.setAutoTrim(true);
        return history;
    }
}
