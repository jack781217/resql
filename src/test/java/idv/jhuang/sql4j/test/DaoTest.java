package idv.jhuang.sql4j.test;

import static idv.jhuang.sql4j.Entity.asEntity;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import idv.jhuang.sql4j.Configuration;
import idv.jhuang.sql4j.Configuration.Type;
import idv.jhuang.sql4j.DaoFactory;
import idv.jhuang.sql4j.DaoFactory.Dao;
import idv.jhuang.sql4j.Entity;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class DaoTest {
	private static final Logger log = LogManager.getLogger(DaoTest.class);
	
	private DaoFactory factory = new DaoFactory(Configuration.parseConfiguration(
			getClass().getClassLoader().getResourceAsStream("sql4j.xml")));
	private Type studentType = factory.types("Student");
	
	public DaoTest() throws Exception {
		
	}
	
	@Before
	public void setup() throws Exception {
		factory.init();
	}
	
	@Test
	public void testCreateMaster() throws Exception {
		
		Dao dao = factory.createDao();
		
		Entity student = dao.createOrUpdate(studentType, asEntity(
				"name", "Jack Huang",
				"age", 24,
				"gender", "male",
				"dateOfBirth", "1989-12-17",
				"hasGraduated", true,
				"car", "Nissan Altima",
				"school", asEntity("name", "UT Austin"),
				"transcript", asEntity("gpa", 3.95),
				"courses", asList(
						asEntity("name", "Microarchitecture"),
						asEntity("name", "Real-Time Operating System")),
				"emails", asList(
						asEntity("address", "jack.huang78@gmail.com"),
						asEntity("address", "jack.huang@utexas.edu")),
				"advisor", asEntity("name", "Sanjay Srinivasan"),
				"dormRoom", asEntity("location", "Off campus")
				));
		dao.commit();
		
		assertNotNull(student);
		assertNotNull(student.get("id"));
		assertNotNull(student.get("transcript"));
		assertNotNull(student.get("school"));
		assertNotNull(student.get("advisor"));
		assertNotNull(student.get("dormRoom"));
		assertNotNull(student.get("emails"));
		assertEquals(2, student.getList("emails", Entity.class).size());
		assertNotNull(student.getList("emails", Entity.class).get(0).get("id"));
		assertNotNull(student.getList("emails", Entity.class).get(1).get("id"));
		assertNotNull(student.get("courses"));
		assertEquals(2, student.getList("courses", Entity.class).size());
		assertNotNull(student.getList("courses", Entity.class).get(0).get("id"));
		assertNotNull(student.getList("courses", Entity.class).get(1).get("id"));
		
		Object studentId = student.get("id");
		Object transcriptId = student.get("transcript", Entity.class).get("id");
		Object schoolId = student.get("school", Entity.class).get("id");
		Object advisorId = student.get("advisor", Entity.class).get("id");
		Object dormRoomId = student.get("dormRoom", Entity.class).get("id");
		Object email1Id = student.getList("emails", Entity.class).get(0).get("id");
		Object email2Id = student.getList("emails", Entity.class).get(1).get("id");
		Object course1Id = student.getList("courses", Entity.class).get(0).get("id");
		Object course2Id = student.getList("courses", Entity.class).get(1).get("id");
		
		List<Entity> students = dao.read(studentType, asList(studentId), asEntity(
				"id", null,
				"name", null, "age", null, "gender", null, "dateOfBirth", null, "hasGraduated", null, "car", null,
				"school", asEntity("id", null, "name", null),
				"transcript", asEntity("id", null, "gpa", null),
				"courses", asEntity("id", null, "name", null),
				"emails", asEntity("id", null, "address", null),
				"advisor", asEntity("id", null, "name", null),
				"dormRoom", asEntity("id", null, "location", null)
				));
		assertNotNull(students);
		assertEquals(1, students.size());
		
		student = students.get(0);
		assertEquals(studentId, student.get("id"));
		assertEquals("Jack Huang", student.get("name"));
		assertEquals(24, (int) student.get("age"));
		assertEquals("male", student.get("gender"));
		assertEquals(true, student.get("hasGraduated"));
		assertEquals("Nissan Altima", student.get("car"));
		
		
		Entity transcript = student.get("transcript");
		assertNotNull(transcript);
		assertEquals(transcriptId, transcript.get("id"));
		assertEquals(3.95, transcript.get("gpa"), 0.0);
		
		Entity school = student.get("school");
		assertNotNull(school);
		assertEquals(schoolId, school.get("id"));
		assertEquals("UT Austin", school.get("name"));
		
		Entity advisor = student.get("advisor");
		assertNotNull(advisor);
		assertEquals(advisorId, advisor.get("id"));
		assertEquals("Sanjay Srinivasan", advisor.get("name"));
		
		Entity dormRoom = student.get("dormRoom");
		assertNotNull(dormRoom);
		assertEquals(dormRoomId, dormRoom.get("id"));
		assertEquals("Off campus", dormRoom.get("location"));
		
		List<Entity> emails = student.get("emails");
		assertNotNull(emails);
		assertEquals(2, emails.size());
		Entity email1 = emails.get(0);
		Entity email2 = emails.get(1);
		assertNotNull(email1);
		assertNotNull(email2);
		assertEquals(email1Id, email1.get("id"));
		assertEquals(email2Id, email2.get("id"));
		
		List<Entity> courses = student.get("courses");
		assertNotNull(courses);
		assertEquals(2, courses.size());
		Entity course1 = courses.get(0);
		Entity course2 = courses.get(1);
		assertNotNull(course1);
		assertNotNull(course2);
		assertEquals(course1Id, course1.get("id"));
		assertEquals(course2Id, course2.get("id"));
		/*
		
		assertNotNull(studentIds.get("id"));
		assertTrue(studentIds.get("id", int.class) > 0);
		dao.commit();
		
		Object id = studentIds.get("id");
		List<Entity> students = dao.read(studentType, asList(id), asEntity(
				"id", null,
				"name", null, "age", null, "gender", null, "dateOfBirth", null, "hasGraduated", null, "car", null,
				"school", asEntity("id", null, "name", null),
				"transcript", asEntity("id", null, "gpa", null),
				"courses", asEntity("id", null, "name", null),
				"emails", asEntity("id", null, "address", null),
				"advisor", asEntity("id", null, "name", null),
				"dormRoom", asEntity("id", null, "location", null)
				));
		assertNotNull(students);
		assertEquals(1, students.size());
		
		
		
		Entity student = students.get(0);
		assertEquals(studentIds.get("id"), student.get("id"));
		assertEquals("Jack Huang", student.get("name"));
		assertEquals(24, (int) student.get("age"));
		assertEquals("male", student.get("gender"));
		assertEquals("1989-12-17", student.get("dateOfBirth").toString());
		assertEquals(true, student.get("hasGraduated"));
		assertEquals("Nissan Altima", student.get("car"));
		
		Entity school = student.get("school");
		Entity schoolIds = studentIds.get("school");
		assertNotNull(school);
		assertNotNull(schoolIds);
		assertEquals(schoolIds.get("id"), school.get("id"));
		assertEquals("UT Austin", school.get("name"));
		
		Entity transcript = student.get("transcript");
		Entity transcriptIds = studentIds.get("transcript");
		assertNotNull(transcript);
		assertNotNull(transcriptIds);
		assertEquals(transcriptIds.get("id"), transcript.get("id"));
		assertEquals(3.95, (double) transcript.get("gpa"), 0.0);
		
		List<Entity> emails = student.get("emails");
		List<Entity> emailsIds = studentIds.get("emails");
		assertNotNull(emails);
		assertNotNull(emailsIds);
		assertEquals()
		assertEquals(emailsIds.size(), emails.size());
		
		assertEquals(transcriptIds.get("id"), transcript.get("id"));
		assertEquals(3.95, (double) transcript.get("gpa"), 0.0);
		*/
	}
	
	
	/*@Test
	public void testCreateSparseFields() throws SQLException {
		Dao dao = factory.createDao();
		Entity student = dao.createOrUpdate(studentType, asEntity(
				""
				));
		assertNotNull(student);
		assertNotNull(student.get("id"));
		assertTrue(student.getInt("id") > 0);
		dao.commit();
	}*/
	
	
	//@Test
	public void test1() throws Exception {
		
		factory.init();
		
		
		
		Dao dao = factory.createDao();
		Entity entity = dao.createOrUpdate(studentType, asEntity(
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
		
		
		dao.createOrUpdate(studentType, asEntity("name", "Jeff Huang"));
		dao.commit();
		
		List<Entity> students = dao.read(studentType, asList(1), asEntity(
				"id", "", "name", "", "car", "",
				"advisor", asEntity("name", "", "students", asEntity("name", "")), 
				"dormRoom", asEntity("location", "", "student", asEntity("name", "")),
				"transcript", asEntity("gpa", "", "owner", asEntity("name", "")),
				"school", asEntity("name", "", "students", asEntity("name", "")),
				"emails", asEntity("address", "", "owner", asEntity("name", "")),
				"courses", asEntity("name", "", "students", asEntity("name", ""))
				
				));
		
		
		log.info(students);
		
	}
	
	//@Test
	/*public void test2() throws Exception {
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
		
	}*/
	
	
	
	

}
