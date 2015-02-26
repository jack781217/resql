package idv.jhuang.sql4j.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import idv.jhuang.sql4j.Schema;
import idv.jhuang.sql4j.Schema.Field.Relation;
import idv.jhuang.sql4j.Schema.Model;
import idv.jhuang.sql4j.Schema.Type;

import java.util.Arrays;

import org.junit.Test;

public class SchemaParseTest {

	@Test
	public void test() {
		Model model = Schema.parseSchema(getClass().getClassLoader().getResourceAsStream("sql4j-test.xml"));
		
		assertTrue(model.types.containsKey("Student"));
		assertTrue(model.types.containsKey("School"));
		assertTrue(model.types.containsKey("Transcript"));
		assertTrue(model.types.containsKey("Email"));
		assertTrue(model.types.containsKey("Course"));
		
		Type student = model.types.get("Student");
		Type school = model.types.get("School");
		
		assertTrue(student.fields.containsKey("name"));
		assertTrue(student.fields.containsKey("age"));
		assertTrue(student.fields.containsKey("gender"));
		assertTrue(student.fields.containsKey("dateOfBirth"));
		assertTrue(student.fields.containsKey("hasGraduated"));
		assertTrue(student.fields.containsKey("advisor"));
		
		assertEquals(Type.STRING, student.fields.get("name").type);
		assertEquals(Type.INT, student.fields.get("age").type);
		assertEquals(Type.ENUM, student.fields.get("gender").type);
		assertEquals(Type.DATE, student.fields.get("dateOfBirth").type);
		assertEquals(Type.BOOLEAN, student.fields.get("hasGraduated").type);
		assertEquals(Type.STRING, student.fields.get("advisor").type);
		assertEquals(Arrays.asList("male", "female"), student.fields.get("gender").values);
		assertEquals(true, student.fields.get("advisor").sparse);
		
		assertTrue(student.fields.containsKey("school"));
		assertEquals(school, student.fields.get("school").type);
		assertEquals(Relation.ManyToOne, student.fields.get("school").relation);
		assertEquals("students", student.fields.get("school").remote);
		assertEquals(true, student.fields.get("school").master);
		assertTrue(school.fields.containsKey("students"));
		assertEquals(student, school.fields.get("students").type);
		assertEquals(Relation.OneToMany, school.fields.get("students").relation);
		assertEquals(false, school.fields.get("students").master);
		
		
		//assertEquals(student, school.fields.get("owner"))
		
		/*
		<field name="transcript" type="Transcript" relation="OneToOne" remote="owner" />
		<field name="school" type="School" relation="ManyToOne" remote="students" />
		<field name="courses" type="Course" relation="ManyToMany" remote="students" />
		<field name="emails" type="Email" relation="OneToMany" remote="owner" />*/
		
	}

}
