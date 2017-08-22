package com.bonc.usdp.shell2es;

import com.google.common.collect.ImmutableSet;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by Administrator on 2017/8/21.
 */
public final class Completion {
    private static final Set<String> COMMANDS = ImmutableSet.of(
            "SELECT",
            "SHOW CATALOGS",
            "SHOW COLUMNS",
            "SHOW FUNCTIONS",
            "SHOW PARTITIONS",
            "SHOW SCHEMAS",
            "SHOW SESSION",
            "SHOW TABLES",
            "CREATE TABLE",
            "DROP TABLE",
            "EXPLAIN",
            "DESCRIBE",
            "USE",
            "HELP",
            "QUIT");

    public static Completer commandCompleter() {
        return new StringsCompleter(COMMANDS);
    }

    public static Completer lowerCaseCommandComleter() {
        return new StringsCompleter(COMMANDS.stream()
                .map(s -> s.toLowerCase(Locale.ENGLISH))
                .collect(Collectors.toSet()));
    }
}
