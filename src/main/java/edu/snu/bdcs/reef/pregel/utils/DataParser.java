package edu.snu.bdcs.reef.pregel.utils;

/**
 * Created by puppybit on 14. 12. 6.
 */

public interface DataParser<T> {


    /**
     * @return parsed data as format T
     * @throws ParseException parsing has failed due to incorrect input format
     */
    public T get() throws ParseException;

    /**
     * Parse data input and keep it for later get() calls
     */
    public void parse();

}