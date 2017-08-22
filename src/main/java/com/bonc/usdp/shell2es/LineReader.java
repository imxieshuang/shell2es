package com.bonc.usdp.shell2es;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.Completer;
import jline.console.history.History;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * Created by Administrator on 2017/8/21.
 */
public class LineReader extends ConsoleReader implements Closeable {
    private boolean interrupted;
    public LineReader(History history, Completer... completers) throws IOException {
        setExpandEvents(false);
        setBellEnabled(true);
        setHandleUserInterrupt(true);
        setHistory(history);
        setHistoryEnabled(false);
        for (Completer completer : completers) {
            addCompleter(completer);
        }
    }

    @Override
    public String readLine(String promt,Character mask) throws IOException {
        String line;
        interrupted=false;
        try {
            line=super.readLine(promt,mask);
        }catch (UserInterruptException e){
            interrupted=true;
            return null;
        }

        if (getHistory() instanceof Flushable){
            ((Flushable) getHistory()).flush();
        }
        return line;

    }

    @Override
    public void close() throws IOException {
        shutdown();
    }

    public boolean interrupted(){
        return interrupted;
    }
}
