package org.example;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.uritemplate.UriTemplate;
import io.vertx.uritemplate.Variables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReplacerWebService {

    static Logger logger = LoggerFactory.getLogger(ReplacerWebService.class);

    static ConcurrentHashMap<String, String> cacheMap = new ConcurrentHashMap<>();

    static Vertx vertx1 = Vertx.vertx();

    static WebClient webClient;

    static UriTemplate uriTemplate = UriTemplate.of("/?key={key}");

    public static void start() {
        webClient = WebClient.create(vertx1);
    }

    public static String getReplacement(String key) {
        return cacheMap.computeIfAbsent(key, s -> {
            try {
                return webClient.get(8888, "localhost", uriTemplate.expandToString(Variables.variables().set("key", key)))
                        .send().map(ar -> {
                            String res = ar.bodyAsString();
                            logger.warn("Web Response: " + key + " " + res);
                            return res;
                        }).toCompletionStage().toCompletableFuture().get(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void shutdown() {
        webClient.close().onComplete(x -> vertx1.close());
    }

    public static void main(String[] args) {
        ReplacerWebService.start();
        System.out.println(ReplacerWebService.getReplacement("key1 value1"));
        System.out.println(ReplacerWebService.getReplacement("key2 value2"));
        ReplacerWebService.shutdown();
    }
}
