package idv.jhuang.sql4j.test;

import idv.jhuang.sql4j.Configuration;
import idv.jhuang.sql4j.DaoFactory;
import idv.jhuang.sql4j.DaoFactory.Dao;
import idv.jhuang.sql4j.Entity;

import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class DaoTest {
	private static final Logger log = LogManager.getLogger(DaoTest.class);

	@Test
	public void test1() throws Exception {
		DaoFactory factory = new DaoFactory(Configuration.parseConfiguration(
				getClass().getClassLoader().getResourceAsStream("sql4j.xml")));
		factory.init();
		
		Dao dao = factory.createDao();
		Entity entity = dao.createOrUpdate("Student", Entity.asEntity(
				"name", "Jack Huang",
				"age", 24,
				"gender", "male",
				"dateOfBirth", "1989-12-17",
				"hasGraduated", true,
				
				"school", Entity.asEntity("name", "Stanford"),
				"transcript", Entity.asEntity("gpa", 3.999),
				"courses", Arrays.asList(
						Entity.asEntity("name", "machine learning"),
						Entity.asEntity("name", "data mining")),
				"emails", Arrays.asList(
						Entity.asEntity("address", "jack.huang78@gmail.com"),
						Entity.asEntity("address", "jack.huang@stanford.edu")),
						
				"car", "Nissan Altima",
				"advisor", Entity.asEntity("name", "Sanjay Srinivasan"),
				"dormRoom", Entity.asEntity("location", "Jester 8F")
				
				));
		
		dao.createOrUpdate("Student", Entity.asEntity("name", "Jeff Huang"));
		dao.commit();
		
		List<Entity> students = dao.read("Student", Arrays.asList(1,3), Entity.asEntity(
				"id", "", "name", "", "car", "",
				"advisor", Entity.asEntity("name", ""),  
				"school", Entity.asEntity("name", ""),
				"emails", Entity.asEntity("address", ""),
				"transcript", Entity.asEntity("gpa", ""),
				"courses", Entity.asEntity("name", "")
				
				));
		
		
		log.info(students);
		
	}
	
	//@Test
	public void test2() throws Exception {
		DaoFactory factory = new DaoFactory(Configuration.parseConfiguration(
				getClass().getClassLoader().getResourceAsStream("sql4j.xml")));
		factory.init();
		
		Dao dao = factory.createDao();
		Entity entity = dao.createOrUpdate("Transcript", Entity.asEntity(
				"gpa", 3.99,
				"owner", Entity.asEntity("name", "s1")));
		entity = dao.createOrUpdate("DormRoom", Entity.asEntity(
				"location", "Jester 9F",
				"student", Entity.asEntity("name", "s2")));
		entity = dao.createOrUpdate("Advisor", Entity.asEntity(
				"name", "Sanjay Srinivasan",
				"students", Arrays.asList(Entity.asEntity("name", "s3"), Entity.asEntity("name", "s4"))));
	
		entity = dao.createOrUpdate("School", Entity.asEntity( 
				"name", "Stanford",
				"students", Arrays.asList(Entity.asEntity("name", "s5"), Entity.asEntity("name", "s6"))
				));
		
		entity = dao.createOrUpdate("Course", Entity.asEntity(
				"name", "Machine Learning",
				"students", Arrays.asList(Entity.asEntity("name", "s7"), Entity.asEntity("name", "s8"))
				));
		entity = dao.createOrUpdate("Email", Entity.asEntity(
				"address", "jack.huang78@gmail.com",
				"owner", Entity.asEntity("name", "s9")
				));
		
		dao.commit();
		
	}
	
	
	
	

}
