package idv.jhuang.sql4j.test;

import idv.jhuang.sql4j.Configuration;
import idv.jhuang.sql4j.DaoFactory;
import idv.jhuang.sql4j.DaoFactory.Dao;

import org.junit.Test;

public class DaoTest {

	@Test
	public void test() throws Exception {
		DaoFactory factory = new DaoFactory(Configuration.parseConfiguration(
				getClass().getClassLoader().getResourceAsStream("sql4j-test.xml")));
		factory.init();
	}

}
