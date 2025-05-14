package cz.vut.fit.domainradar.standalone;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Interface for defining a generic collector.
 *
 * @author Matěj Čech
 */
interface CollectorInterface {
    String getName();
    void addOptions(@NotNull Options options);
    void run(CommandLine cmd);
    void close() throws IOException;
}