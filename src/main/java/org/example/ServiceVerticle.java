package org.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceVerticle extends AbstractVerticle {

    static Logger logger = LoggerFactory.getLogger(ServiceVerticle.class);

    @Override
    public void start() throws Exception {
        // Create a Router
        Router router = Router.router(vertx);

        // Mount the handler for all incoming requests at every path and HTTP method
        router.route().handler(context -> {
            // Get the address of the request
            // String address = context.request().connection().remoteAddress().toString();
            // Get the query parameter "name"
            MultiMap queryParams = context.queryParams();
            String key = queryParams.contains("key") ? queryParams.get("key") : "unknown";
            logger.warn("{} {}", key, key.replace("key", "replace"));
            // Write a response
            context.response().send(key.replace("key", "replace"));
        });

        // Create the HTTP server
        vertx.createHttpServer()
                // Handle every request using the router
                .requestHandler(router)
                // Start listening
                .listen(8888)
                // Print the port
                .onSuccess(server ->
                        logger.warn(
                                "HTTP server started on port " + server.actualPort()
                        )
                );
    }

    public static void main(String[] args) {
        Vertx vertx1 = Vertx.vertx();
        Runtime.getRuntime().addShutdownHook(new Thread(vertx1::close));
        vertx1.deployVerticle(new ServiceVerticle());
    }
}
