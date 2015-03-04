package idv.jhuang.sql4j.test;

import idv.jhuang.sql4j.controller.DatabaseController;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class DatabaseControllerTest {
	private static final Logger log = LogManager
			.getLogger(DatabaseControllerTest.class);
	
	@Test
	public void test() throws ClassNotFoundException {
		DatabaseController ctrl = new DatabaseController();
		log.info("\n" + ctrl.schema().getEntity());
	}

}
