package idv.jhuang.sql4j.test;

import static java.util.Arrays.*;
import static idv.jhuang.sql4j.Entity.*;

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
		Entity entity = dao.createOrUpdate("Student", asEntity(
				"name", "Jack Huang",
				"age", 24,
				"gender", "male",
				"dateOfBirth", "1989-12-17",
				"hasGraduated", true,
				
				"school", asEntity("name", "Stanford"),
				"transcript", asEntity("gpa", 3.999),
				"courses", asList(
						asEntity("name", "machine learning"),
						asEntity("name", "data mining")),
				"emails", asList(
						asEntity("address", "jack.huang78@gmail.com"),
						asEntity("address", "jack.huang@stanford.edu")),
						
				"car", "Nissan Altima",
				"advisor", asEntity("name", "Sanjay Srinivasan"),
				"dormRoom", asEntity("location", "Jester 8F")
				
				));
		log.info(entity);
		
		
		dao.createOrUpdate("Student", asEntity("name", "Jeff Huang"));
		dao.commit();
		
		List<Entity> students = dao.read("Student", asList(1), asEntity(
				"id", "", "name", "", "car", "",
				"advisor", asEntity("name", "", "students", asEntity("name", "")),  
				"transcript", asEntity("gpa", "", "owner", asEntity("name", "")),
				"school", asEntity("name", "", "students", asEntity("name", "")),
				"emails", asEntity("address", "", "owner", asEntity("name", "")),
				"courses", asEntity("name", "", "students", asEntity("name", ""))
				
				));
		
		
		log.info(students);
		
	}
	
	//@Test
	public void test2() throws Exception {
		DaoFactory factory = new DaoFactory(Configuration.parseConfiguration(
				getClass().getClassLoader().getResourceAsStream("sql4j.xml")));
		factory.init();
		
		Dao dao = factory.createDao();
		Entity entity = dao.createOrUpdate("Transcript", asEntity(
				"gpa", 3.99,
				"owner", asEntity("name", "s1")));
		entity = dao.createOrUpdate("DormRoom", asEntity(
				"location", "Jester 9F",
				"student", asEntity("name", "s2")));
		entity = dao.createOrUpdate("Advisor", asEntity(
				"name", "Sanjay Srinivasan",
				"students", asList(asEntity("name", "s3"), asEntity("name", "s4"))));
	
		entity = dao.createOrUpdate("School", asEntity( 
				"name", "Stanford",
				"students", asList(asEntity("name", "s5"), asEntity("name", "s6"))
				));
		
		entity = dao.createOrUpdate("Course", asEntity(
				"name", "Machine Learning",
				"students", asList(asEntity("name", "s7"), asEntity("name", "s8"))
				));
		entity = dao.createOrUpdate("Email", asEntity(
				"address", "jack.huang78@gmail.com",
				"owner", asEntity("name", "s9")
				));
		
		dao.commit();
		
	}
	
	
	
	

}
