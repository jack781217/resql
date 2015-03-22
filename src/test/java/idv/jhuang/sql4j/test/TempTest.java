package idv.jhuang.sql4j.test;

import idv.jhuang.sql4j.Entity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class TempTest {
	private static final Logger log = LogManager.getLogger(TempTest.class);
	
	
	@Test
	public void test() throws Exception {
		
		log.info(new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(
				Entity.entity("name", "a", "age", 10)));
		
		
	}

}
