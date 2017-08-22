package com.bonc.usdp.shell2es;

/**
 * Created by Administrator on 2017/8/21.
 */
public final class Help {
    private Help() {
    }

    public static String getHelpText(){
        return "" +
                "Supported commands:\n" +
                "QUIT\n" +
                "EXPLAIN [ ( option [, ...] ) ] <query>\n" +
                "    options: FORMAT { TEXT | GRAPHVIZ }\n" +
                "             TYPE { LOGICAL | DISTRIBUTED }\n" +
                "DESCRIBE <table>\n" +
                "SHOW COLUMNS FROM <table>\n" +
                "SHOW FUNCTIONS\n" +
                "SHOW CATALOGS [LIKE <pattern>]\n" +
                "SHOW SCHEMAS [FROM <catalog>] [LIKE <pattern>]\n" +
                "SHOW TABLES [FROM <schema>] [LIKE <pattern>]\n" +
                "SHOW PARTITIONS FROM <table> [WHERE ...] [ORDER BY ...] [LIMIT n]\n" +
                "USE [<catalog>.]<schema>\n" +
                "";
    }
}
