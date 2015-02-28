package idv.jhuang.sql4j;

import static com.google.common.base.Preconditions.checkArgument;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Sql {
	private static final Logger log = LogManager.getLogger(Sql.class);
	
	private Connection conn;
	public Sql(Connection conn) {
		this.conn = conn;
	}
	
	public void resetDatabase(String name) throws SQLException {
		try(Statement stmt = conn.createStatement()) {
			String sql;
			
			sql = String.format("DROP DATABASE IF EXISTS %s;", name);
			log.debug("SQL> {}", sql);
			stmt.execute(sql);
			
			sql = String.format("CREATE DATABASE %s;", name);
			log.debug("SQL> {}", sql);
			stmt.execute(sql);
			
			sql = String.format("USE %s;", name);
			log.debug("SQL> {}", sql);
			stmt.execute(sql);
		}
	}
	
	public void createTable(String table, String column, String type, boolean pk, boolean auto) throws SQLException {
		checkArgument(!(auto && !pk), "A column cannot be auto-generated but not primary key");
		
		try(Statement stmt = conn.createStatement()) {
			
			String sql;
			
			if(auto) {
				sql = String.format("CREATE TABLE %s(%s %s AUTO_INCREMENT, PRIMARY KEY(%s))", table, column, type, column);
			} else if(pk) {
				sql = String.format("CREATE TABLE %s(%s %s, PRIMARY KEY(%s));", table, column, type, column);
			} else {
				sql = String.format("CREATE TABLE %s(%s %s);", table, column, type);
			}
					
					
			
			log.debug("SQL> {}", sql);
			stmt.execute(sql);
		}
	}
	
	public void alterTableAdd(String table, String column, String type, boolean unique) throws SQLException {
		try(Statement stmt = conn.createStatement()) {
			String sql;
			
			sql = String.format("ALTER TABLE %s ADD %s %s;", table, column, type);
			log.debug("SQL> {}", sql);
			stmt.execute(sql);
			
			if(unique) {
				sql = String.format("ALTER TABLE %s ADD UNIQUE(%s);", table, column);
				log.debug("SQL> {}", sql);
				stmt.execute(sql);
			}
		}
	}
	
	public void alterTableAdd(String table, String fkName, String fkType, String refTable, String refName, boolean unique) throws SQLException {
		try(Statement stmt = conn.createStatement()) {
			String sql;
			
			sql = String.format("ALTER TABLE %s ADD %s %s;", table, fkName, fkType);
			log.debug("SQL> {}", sql);
			stmt.execute(sql);
			
			sql = String.format("ALTER TABLE %s ADD FOREIGN KEY(%s) REFERENCES %s(%s);",
					table, fkName, refTable, refName);
			log.debug("SQL> {}", sql);
			stmt.execute(sql);
			
			if(unique) {
				sql = String.format("ALTER TABLE %s ADD UNIQUE(%s);", table, fkName);
				log.debug("SQL> {}", sql);
				stmt.execute(sql);
			}
			
		}
	}
	
	
}
