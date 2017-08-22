package com.bonc.usdp.shell2es;

import io.airlift.airline.SingleCommand;

/**
 * Created by Administrator on 2017/8/21.
 */

public class Shell {
    public static void main(String[] args) {
        Console console = SingleCommand.singleCommand(Console.class).parse(args);
        if (console.helpOption.showHelpIfRequested()) {
            return;
        }
        console.run();
    }
}
