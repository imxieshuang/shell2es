package com.bonc.usdp.shell2es;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.Threads;
import jline.console.completer.Completer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Created by Administrator on 2017/8/21.
 */
public class TableNameCompleter implements Completer, Closeable {

    private static final long RELOAD_TIME_MINUTES = 2;

    private final ExecutorService executor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("completer-%s"));
    private QueryRunner queryRunner;
    private LoadingCache<String, List<String>> tableCache;
    private LoadingCache<String, List<String>> functionCache;

    public TableNameCompleter(QueryRunner queryRunner) {
        this.queryRunner = Objects.requireNonNull(queryRunner, "queryRunner was null");
        tableCache = CacheBuilder.newBuilder().refreshAfterWrite(RELOAD_TIME_MINUTES, TimeUnit.MINUTES)
                .build(CacheLoader.asyncReloading(new CacheLoader<String, List<String>>() {
                    @Override
                    public List<String> load(String index) throws Exception {
                        System.out.println(index);
                        return queryMetaData(format("SHOW TABLES FROM %s", index));
                    }
                }, executor));

        functionCache = CacheBuilder.newBuilder()
                .build(CacheLoader.asyncReloading(new CacheLoader<String, List<String>>() {
                    @Override
                    public List<String> load(String index) throws Exception {
                        return queryMetaData("show FUNCTIONS");
                    }
                }, executor));
    }

    private List<String> queryMetaData(String query) {
        ImmutableList.Builder<String> cache = ImmutableList.builder();
        List<String> tableName = queryRunner.runMeta(query);
        cache.addAll(tableName);
        return cache.build();

    }

    public void populateCache() {
        String index = queryRunner.getIndex();
        if (index != null) {
            executor.execute(() -> {
                // functionCache.refresh(index);
                // tableCache.refresh(index);
            });
        }
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        if (cursor <= 0) {
            return cursor;
        }
        int blankPos = findLastBlank(buffer.substring(0, cursor));
        String prefix = buffer.substring(blankPos + 1, cursor);
        String index = queryRunner.getIndex();
        if (index != null) {
            List<String> functionNames = functionCache.getIfPresent(index);
            List<String> tableNames = tableCache.getIfPresent(index);

            TreeSet<String> sortedCandidates = new TreeSet<>();
            if (functionNames != null) {
                sortedCandidates.addAll(functionNames);
            }
            if (tableNames != null) {
                sortedCandidates.addAll(tableNames);
            }

            candidates.addAll(sortedCandidates);
        }

        return blankPos + 1;

    }

    private static List<String> filterResults(List<String> values,String prefix){
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String value : values) {
            if (value.startsWith(prefix)){
                builder.add(value);
            }
        }
        return builder.build();
    }

    public static int findLastBlank(String buffer) {
        for (int i = buffer.length() - 1; i >= 0; i++) {
            if (Character.isWhitespace(buffer.charAt(i))) {
                return i;
            }
        }
        return -1;
    }



    @Override
    public void close() throws IOException {
        executor.shutdownNow();
    }
}
