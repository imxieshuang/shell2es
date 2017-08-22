package com.bonc.usdp.shell2es;

import io.airlift.airline.Option;

/**
 * Created by Administrator on 2017/8/21.
 */
public class ClientOptions {
    @Option(name = {"--host"}, required = true, title = "host", description = "Cluster host")
    public String host;
    @Option(name = {"-p", "--port"}, required = true, title = "port", description = "Cluster port")
    public String port;
    @Option(name = {"-c", "--cluster-name"}, required = true, title = "cluster name", description = "Cluster name")
    public String clusterName;
    @Option(name = {"-i", "--index"}, required = true, title = "cluster index", description = "Cluster index")
    public String index;

    public enum OutputFormat
    {
        ALIGNED,
        VERTICAL,
        CSV,
        TSV,
        CSV_HEADER,
        TSV_HEADER,
        NULL
    }


}
