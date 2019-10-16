package iisc.dsl.codd.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

/**
 * @author dk
 */

public class DateParser {
	private static final List<String> formatStrings = Arrays.asList("yyyy-MM-dd", "dd-MM-yyyy", "dd-MMM-yyyy", "dd-MM-yy", "dd-MMM-yy");
	
	public static LocalDate parse(String val) {
    	LocalDate ld = null;
    	for (String format: formatStrings) {
    		try {
        		ld = LocalDate.parse(val, DateTimeFormatter.ofPattern(format));
        		return ld;
        	} catch (DateTimeParseException e) { }
		}
    	throw new RuntimeException("Unable to parse date: " + val);
	}
}
