package cz.vut.fit.domainradar.standalone.https;

import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Scanner;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;

public class HTTPSFetcherRepl {
    public static void main(String[] args) {
        var maxRedirects = 2;
        var timeoutMs = 3000;
        var executor = Executors.newVirtualThreadPerTaskExecutor();

        var logger = LoggerFactory.getLogger(HTTPSFetcherRepl.class);

        var fetcher = new HTTPSFetcherImpl(maxRedirects, timeoutMs, executor, logger);
        Scanner scanner = new Scanner(System.in);

        while (true) {
            String inputLine;
            if (System.console() != null) {
                inputLine = System.console().readLine("> ").trim();
            } else {
                System.out.print("> ");
                inputLine = scanner.nextLine().trim();
            }

            if (inputLine.startsWith(":mr")) {
                maxRedirects = Integer.parseInt(inputLine.substring(3).trim());
                fetcher = new HTTPSFetcherImpl(maxRedirects, timeoutMs, executor, logger);
            } else if (inputLine.startsWith(":t")) {
                timeoutMs = Integer.parseInt(inputLine.substring(2).trim());
                fetcher = new HTTPSFetcherImpl(maxRedirects, timeoutMs, executor, logger);
            } else {
                // Resolve hostname stored in inputLine to IP address
                InetAddress inetAddress = null;
                try {
                    inetAddress = InetAddress.getByName(inputLine.trim());
                } catch (UnknownHostException e) {
                    logger.error("Unknown host");
                    continue;
                }

                var ip = inetAddress.getHostAddress();
                var result = fetcher.collect(inputLine, ip);
                logger.info("Status: {}, error: {}, certs: {}, HTML fetched: {}",
                        result.statusCode(), result.error(),
                        result.tlsData() != null ? result.tlsData().certificates().size() : 0,
                        result.html() != null);

                if (result.html() == null) {
                    logger.info("Trying HTTP fetch for HTML");
                    try {
                        var html = fetcher.collectHTTPOnly(inputLine, ip).join();
                        if (html != null) {
                            logger.info("HTML fetched successfully.");
                        } else {
                            logger.info("Failed to fetch HTML.");
                        }
                    } catch (CompletionException e) {
                        logger.error("Failed to fetch HTML: {}", e.getCause().getMessage());
                    }
                }
            }
        }
    }
}
