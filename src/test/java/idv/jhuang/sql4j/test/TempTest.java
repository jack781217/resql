package idv.jhuang.sql4j.test;

import java.util.Arrays;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class TempTest {
	private static final Logger log = LogManager.getLogger(TempTest.class);
	
	
	@Test
	public void test() {
		
		
		log.info(String.join(",", Collections.nCopies(1, "?")));
		
		String str = String.join(", ", Collections.nCopies(1, 
				"(" + String.join(", ", Collections.nCopies(1, "?")) + ")" ));
		
		log.info(str);
		
		
	}

}
