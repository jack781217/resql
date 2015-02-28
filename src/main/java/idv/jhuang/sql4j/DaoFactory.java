package idv.jhuang.sql4j;

import idv.jhuang.sql4j.Configuration.Field;
import idv.jhuang.sql4j.Configuration.Type;
import idv.jhuang.sql4j.Configuration.Field.Relation;
import static idv.jhuang.sql4j.Configuration.Field.Relation.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DaoFactory {
	private static final Logger log = LogManager.getLogger(DaoFactory.class);
	
	private Configuration config;
	
	public DaoFactory(Configuration config) {
		this.config = config;
	}
	
	public void init() throws SQLException {
		try(Connection conn = DriverManager.getConnection(config.database.url, config.database.user, config.database.password)) {
			Sql sql = new Sql(conn);
			sql.resetDatabase(config.database.name);
			
			for(Type type : config.model.types.values()) {
				sql.createTable(type.name, type.id.name, sqlDataType(type.id), true, true);
			}
			
			for(Type type : config.model.types.values()) {
				
				
				for(Field field : type.fields.values()) {
					if(!field.master)
						continue;
					
					
					if(!field.sparse) {
						String tableName = type.name;
						
						if(field.relation == None) {
							sql.alterTableAdd(tableName, field.name, sqlDataType(field), false);
						} else {
							boolean columnUnique = (field.relation == OneToOne);
							sql.alterTableAdd(tableName, sqlFKName(field), sqlDataType(field.type.id), 
									field.type.name, field.type.id.name, columnUnique);
						}
						
							
					} else {
						String joinTableName = sqlJoinTableName(type, field);
						String idName = type.id.name;
						String idType = sqlDataType(type.id);
						boolean idPK = (field.relation == None 
								|| field.relation == OneToOne 
								|| field.relation == ManyToOne);
						
						sql.createTable(joinTableName, idName, idType, idPK, false);
						
						
						if(field.relation == None) {
							sql.alterTableAdd(joinTableName, field.name, sqlDataType(field), false);
						} else {
							boolean columnUnique = (field.relation == OneToOne || field.relation == OneToMany);
							sql.alterTableAdd(joinTableName, sqlFKName(field), sqlDataType(field.type.id), 
									field.type.name, field.type.id.name, columnUnique);
						}
						
					}
					
					
					
					/*
					if(field.master) {
						switch(field.relation) {
						case None:
							if(!field.sparse) {
								sql.alterTableAdd(type.name, field.name, sqlDataType(field.type, field.values), false);	//TODO implement unique
							} else {
								sql.createTable(sqlJoinTableName(type, field), id.name, sqlDataType(id.type, null), true, false);
								sql.alterTableAdd(sqlJoinTableName(type, field), field.name, sqlDataType(field.type, field.values ), false);	//TODO unique
							}	
							break;
						case OneToOne:
							if(!field.sparse) {
								sql.alterTableAdd(type.name, sqlFKName(field), sqlDataType(field.type.id.type, null), field.type.name, field.type.id.name, true);
							} else {
								sql.createTable(sqlJoinTableName(type, field), id.name, sqlDataType(id.type, null), true, false);
								sql.alterTableAdd(type.name, sqlFKName(field), sqlDataType(field.type.id.type, null), field.type.name, field.type.id.name, true);								
							}
							break;
						case ManyToOne:
							if(!field.sparse) {
								sql.alterTableAdd(type.name, sqlFKName(field), sqlDataType(field.type.id.type, null), field.type.name, field.type.id.name, false);
							} else {
								sql.createTable(sqlJoinTableName(type, field), id.name, sqlDataType(id.type, null), true, false);
								sql.alterTableAdd(type.name, sqlFKName(field), sqlDataType(field.type.id.type, null), field.type.name, field.type.id.name, false);
							}
							break;
						case OneToMany:
							sql.createTable(sqlJoinTableName(type, field), id.name, sqlDataType(id.type, null), false, false);
							sql.alterTableAdd(sqlJoinTableName(type, field), sqlFKName(field), sqlDataType(field.type.id.type, null), field.type.name, field.type.id.name, true);
							break;
						case ManyToMany:
							sql.createTable(sqlJoinTableName(type, field), id.name, sqlDataType(id.type, null), false, false);
							sql.alterTableAdd(sqlJoinTableName(type, field), sqlFKName(field), sqlDataType(field.type.id.type, null), field.type.name, field.type.id.name, false);
							break;
						}
					}*/
				}
			}
		}
	}
	
	private String sqlDataType(Field field) {
		if(field.type == Type.INT) {
			return "INTEGER";
		} else if(field.type == Type.DOUBLE) {
			return "DOUBLE";
		} else if(field.type == Type.BOOLEAN) {
			return "BOOLEAN";
		} else if(field.type == Type.STRING) {
			return "VARCHAR(255)";
		} else if(field.type == Type.DATE) {
			return "DATE";
		} else if(field.type == Type.ENUM) {
			return String.format("ENUM('%s')", String.join("','", field.values));
		} else {
			return "INTEGER";
		}
	}
	
	private String sqlJoinTableName(Type type, Field field) {
		return String.format("%s_%s", type.name, field.name);
	}
	
	private String sqlFKName(Field field) {
		return String.format("%s_id", field.name);
	}
	
	
	
	public Dao createDao() throws SQLException {
		return new Dao();
	}
	
	
	
	public class Dao implements AutoCloseable {
		private final Logger log = LogManager.getLogger(DaoFactory.Dao.class);
		
		private Connection conn;
		
		private Dao() throws SQLException {
			conn = openConnection();
		}
		
		private Connection openConnection() throws SQLException {
			//log.info("url={}, user={}, password={}", config.database.url, config.database.password);
			Connection conn = DriverManager.getConnection(config.database.url, config.database.user, config.database.password);
			conn.setAutoCommit(false);
			return conn;
		}
		
		
		
		
		
		
		public void commit() throws SQLException {
			conn.commit();
			conn.close();
			conn = openConnection();
		}
		
		@Override
		public void close() {
			try {
				conn.close();
			} catch(SQLException e) {
				log.warn("Error closing connection.");
			}
			
		}
	}
	
	
	
}
