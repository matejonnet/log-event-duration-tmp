package org.jboss.pnc.logprocessor.eventduration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ales Justin
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Application application = new Application(args);
        Thread runner = new Thread(application::start);
        Runtime.getRuntime().addShutdownHook(new Thread(application::stop));
        runner.start();
        log.info("Application started ...");
    }
}
