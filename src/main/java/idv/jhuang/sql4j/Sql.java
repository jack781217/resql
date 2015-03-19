package idv.jhuang.sql4j;

import static com.google.common.base.Preconditions.checkArgument;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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
	
	
	
	public List<Object> insertInto(String table, List<String> columns, List<String> types, List<List<Object>> valuess) throws SQLException {
		String sql = String.format("INSERT INTO %s(%s) VALUES %s;", table, 
				String.join(", ", columns),
				String.join(", ", Collections.nCopies(valuess.size(), 
						"(" + String.join(", ", Collections.nCopies(columns.size(), "?")) + ")" )));
		
		if(valuess.isEmpty())
			return new ArrayList<>();
		
		List<Object> ids = new ArrayList<>();
		try(PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			
			for(int i = 0, k = 1; i < valuess.size(); i++) {
				List<Object> values = valuess.get(i);
				for(int j = 0; j < values.size(); j++, k++) {
					setParameter(pstmt, k, values.get(j), types.get(j));
				}
			}
			
			log.debug("SQL> {}", sql);
			pstmt.executeUpdate();
			ResultSet rs = pstmt.getGeneratedKeys();
			while(rs.next())
				ids.add(rs.getInt(1));
		}
		
		return ids;
	}
	
	/*public List<List<Object>> selectFrom(String table, List<String> columns, List<String> types, boolean and) throws SQLException {
		return selectFrom(table, columns, types, Arrays.asList(), Arrays.asList(), Arrays.asList(), and);
	}*/
	
	
	public List<List<Object>> selectFrom(String table, List<String> columns, List<String> types, 
			List<String> critColumns, List<String> critTypes, List<String> critConds,
			List<List<String>> critOpss, List<List<Object>> critValuess) throws SQLException {
		
		if(critValuess == null) {
			critValuess = new ArrayList<>();
		}
		
		String critStr = "";
		for(int i = 0; i < critValuess.size(); i++) {
			if(i > 0)
				critStr += ") AND (";
			for(int j = 0; j < critValuess.get(i).size(); j++) {
				if(j > 0)
					critStr += " " + critConds.get(i) + " ";
				critStr += critColumns.get(i) + " " + critOpss.get(i).get(j) + " " + "?";
			}
		}
		if(!critStr.isEmpty())
			critStr = "(" + critStr + ")";
		
		
		String sql = critStr.isEmpty() ?
				String.format("SELECT %s FROM %s;", String.join(", ", columns), table) :
				String.format("SELECT %s FROM %s WHERE %s;",
				String.join(", ", columns), table,
				critStr
				);
		
		//log.debug("SQL> {}", sql);
		
		List<List<Object>> rows = new ArrayList<>();
		try(PreparedStatement pstmt = conn.prepareStatement(sql)) {
			
			for(int i = 0, k = 0; i < critColumns.size(); i++)
				for(int j = 0; j < critValuess.get(i).size(); j++, k++) {
					setParameter(pstmt, k + 1, 
							critValuess.get(i).get(j),
							critTypes.get(i));
				}
				
			
			log.debug("SQL> {}", sql);
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				List<Object> row = new ArrayList<>();
				for(int i = 0; i < columns.size(); i++)
					row.add(getParameter(rs, i + 1, types.get(i)));
				rows.add(row);
			}
			
			
		}
		
		
		return rows;
		
	}
	
	/*public List<List<Object>> selectFrom(String table, List<String> columns, List<String> types,
			List<String> critColumns, List<String> critTypes, List<Object> critValues, boolean and) throws SQLException {
		String condOp = and ? "AND" : "OR";
		String sql = critColumns.isEmpty() ?
				String.format("SELECT %s FROM %s;", 
						String.join(", ", columns), table) :	
				String.format("SELECT %s FROM %s WHERE %s;", 
						String.join(", ", columns), table, String.join("=? " + condOp, critColumns) + "=?");
		
		List<List<Object>> rows = new ArrayList<>();
		try(PreparedStatement pstmt = conn.prepareStatement(sql)) {
			
			for(int i = 0; i < critColumns.size(); i++) 
				setParameter(pstmt, i + 1, critValues.get(i), critTypes.get(i));
			
			log.debug("SQL> {}", sql);
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				List<Object> row = new ArrayList<>();
				for(int i = 0; i < columns.size(); i++)
					row.add(getParameter(rs, i + 1, types.get(i)));
				rows.add(row);
			}
			
			
		}
		
		
		return rows;
	}*/
	
	public void updateSet(String table, String column, String type, Object value, String idColumn, String idType, Object idValue) throws SQLException {
		updateSet(table, Arrays.asList(column), Arrays.asList(type), Arrays.asList(value), idColumn, idType, idValue);
	}
	public void updateSet(String table, List<String> columns, List<String> types, List<Object> values, 
			String idColumn, String idType, Object idValue) throws SQLException {
		
		String sql = String.format("UPDATE %s SET %s WHERE %s=?;",
				table, 
				String.join("=?, ", columns) + "=?",
				idColumn);
		try(PreparedStatement pstmt = conn.prepareStatement(sql)) {
			for(int i = 0; i < columns.size(); i++) {
				setParameter(pstmt, i + 1, values.get(i), types.get(i));
			}
			setParameter(pstmt, columns.size() + 1, idValue, idType);
			log.debug("SQL> {}", sql);
			pstmt.executeUpdate();
		}
	}
	public void deleteFrom(String table, String column, String type, Object values) throws SQLException {
		deleteFrom(table, Arrays.asList(column), Arrays.asList(type), Arrays.asList(values));
	}
	public void deleteFrom(String table, List<String> columns, List<String> types, List<Object> values) throws SQLException {
		String sql = String.format("DELETE FROM %s WHERE %s;", 
				table, 
				String.join("=? AND", columns) + "=?");
		try(PreparedStatement pstmt = conn.prepareStatement(sql)) {
			for(int i = 0; i < columns.size(); i++) {
				setParameter(pstmt, i + 1, values.get(i), types.get(i));
			}
			log.debug("SQL> {}", sql);
			pstmt.executeUpdate();
		}
	}
	
	/**
	 * Test existence
	 */
	public int selectExist(String table, String idColumn, String idType, Object idValue) throws SQLException {
		String sql = String.format("SELECT EXISTS(SELECT 1 FROM %s WHERE %s=? LIMIT 1);",
				table, idColumn);
		
		try(PreparedStatement pstmt = conn.prepareStatement(sql)) {
			setParameter(pstmt, 1, idValue, idType);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			return rs.getInt(1);
		}
	}
	
	private void setParameter(PreparedStatement pstmt, int idx, Object value, String type) throws SQLException {
		if(type.startsWith("ENUM("))
			type = "ENUM";
		
		switch(type) {
		case "INTEGER":
			if(value == null)
				pstmt.setNull(idx, Types.INTEGER);
			else
				pstmt.setInt(idx, (Integer)value);
			break;
		case "ENUM":
		case "VARCHAR(255)":
			if(value == null)
				pstmt.setNull(idx, Types.VARCHAR);
			else
				pstmt.setString(idx, (String)value);
			break;
		case "DOUBLE":
			if(value == null)
				pstmt.setNull(idx, Types.DOUBLE);
			else
				pstmt.setDouble(idx, (Double)value);
			break;
		case "BOOLEAN":
			if(value == null)
				pstmt.setNull(idx, Types.BOOLEAN);
			else
				pstmt.setBoolean(idx, (Boolean)value);
			break;
		case "DATE":
			if(value == null)
				pstmt.setNull(idx, Types.DATE);
			else
				pstmt.setDate(idx, Date.valueOf((String)value));
			break;
		default:
			throw new IllegalArgumentException("Unsuppoted SQL type: " + type + ".");
		}
	}
	private Object getParameter(ResultSet rs, int idx, String type) throws SQLException {
		if(type.startsWith("ENUM("))
			type = "ENUM";
		
		
		switch(type) {
		case "INTEGER":
			return (Integer) rs.getInt(idx);
		case "ENUM":
		case "VARCHAR(255)":
			return (String) rs.getString(idx);
		case "DOUBLE":
			return (Double) rs.getDouble(idx);
		case "BOOLEAN":
			return (Boolean) rs.getBoolean(idx);
		case "DATE":
			return (Date) rs.getDate(idx);
		default:
			throw new IllegalArgumentException("Unsuppoted SQL type: " + type + ".");
		}
	}
	
	
}
