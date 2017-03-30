package org.saliya.ndssl;

import org.apache.commons.cli.*;

import java.util.Optional;

/**
 * Saliya Ekanayake on 2/22/17.
 */
public class Utils {
    /**
     * Parse command line arguments
     *
     * @param args Command line arguments
     * @param opts Command line options
     * @return An <code>Optional&lt;CommandLine&gt;</code> object
     */
    public static Optional<CommandLine> parseCommandLineArguments(
            String[] args, Options opts) {

        CommandLineParser optParser = new GnuParser();

        try {
            return Optional.ofNullable(optParser.parse(opts, args));
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    public static double BJ(double alpha, int anomalousCount, int setSize) {
        return setSize * KL(((double) anomalousCount) / setSize, alpha);
    }

    private static double KL(double a, double b) {
        assert(a >= 0 && a <= 1);
        assert(b > 0 && b < 1);

        // corner cases
        if (a == 0) {
            return Math.log(1 / (1 - b));
        }
        if (a == 1) {
            return Math.log(1 / b);
        }

        return a * Math.log(a / b) + (1 - a) * Math.log((1 - a) / (1 - b));
    }

    public static double logb(double n, double b) {
        return Math.log(n) / Math.log(b);
    }

    public static int log2(int x) {
        if (x <= 0) {
            throw new IllegalArgumentException("Error. Argument must be greater than 0. Found " + x);
        }
        int result = 0;
        x >>= 1;
        while (x > 0) {
            result++;
            x >>= 1;
        }
        return result;
    }

    public static String formatElapsedMillis(long elapsed){
        String format = "%dd:%02dH:%02dM:%02dS:%03dmS";
        short millis = (short)(elapsed % (1000.0));
        elapsed = (elapsed - millis) / 1000; // remaining elapsed in seconds
        byte seconds = (byte)(elapsed % 60.0);
        elapsed = (elapsed - seconds) / 60; // remaining elapsed in minutes
        byte minutes =  (byte)(elapsed % 60.0);
        elapsed = (elapsed - minutes) / 60; // remaining elapsed in hours
        byte hours = (byte)(elapsed % 24.0);
        long days = (elapsed - hours) / 24; // remaining elapsed in days
        return String.format(format, days, hours, minutes,  seconds, millis);
    }


}
