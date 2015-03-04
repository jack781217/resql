package idv.jhuang.sql4j.test;

import idv.jhuang.sql4j.Configuration;
import idv.jhuang.sql4j.DaoFactory;
import idv.jhuang.sql4j.DaoFactory.Dao;
import idv.jhuang.sql4j.Entity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class DaoTest {
	private static final Logger log = LogManager.getLogger(DaoTest.class);

	@Test
	public void test() throws Exception {
		DaoFactory factory = new DaoFactory(Configuration.parseConfiguration(
				getClass().getClassLoader().getResourceAsStream("sql4j.xml")));
		factory.init();
		
		Dao dao = factory.createDao();
		Entity entity = dao.create(Entity.asEntity(
				"name", "Jack Huang",
				"age", 24,
				"gender", "male",
				"dateOfBirth", "1989-12-17",
				"hasGraduated", true), "Student");
		dao.commit();
		
		log.info(entity);
		
	}
	
	
	

}
