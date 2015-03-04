package idv.jhuang.sql4j;

import static idv.jhuang.sql4j.Configuration.Field.Relation.ManyToOne;
import static idv.jhuang.sql4j.Configuration.Field.Relation.None;
import static idv.jhuang.sql4j.Configuration.Field.Relation.OneToMany;
import static idv.jhuang.sql4j.Configuration.Field.Relation.OneToOne;
import idv.jhuang.sql4j.Configuration.Field;
import idv.jhuang.sql4j.Configuration.Field.Relation;
import idv.jhuang.sql4j.Configuration.Type;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DaoFactory {
	private static final Logger log = LogManager.getLogger(DaoFactory.class);
	
	public Configuration config;
	
	public DaoFactory(Configuration config) {
		this.config = config;
	}
	
	public void init() throws SQLException {
		log.info("Connecting to database. URL={}, User={}.", config.database.url, config.database.user);
		try(Connection conn = DriverManager.getConnection(config.database.url, config.database.user, config.database.password)) {
			Sql sql = new Sql(conn);
			sql.resetDatabase(config.database.name);
			
			// first round: create all main tables
			for(Type type : config.model.types.values()) {
				sql.createTable(type.name, type.id.name, sqlDataType(type.id), true, true);
			}
			
			// second round: create columns and join tables
			for(Type type : config.model.types.values()) {
				for(Field field : type.fields.values()) {
				
					if(!field.master)
						continue;
					
					// add columns in main table
					if(!field.sparse) {
						String tableName = type.name;
						
						if(field.relation == None) {
							sql.alterTableAdd(tableName, field.name, sqlDataType(field), false);
						} else {
							boolean columnUnique = (field.relation == OneToOne);
							sql.alterTableAdd(tableName, sqlFKName(field), sqlDataType(field.type.id), 
									field.type.name, field.type.id.name, columnUnique);
						}
						
					// create join table	
					} else {
						String joinTableName = sqlJoinTableName(type, field);
						
						boolean idPK = (field.relation == None 
								|| field.relation == OneToOne 
								|| field.relation == ManyToOne);
						sql.createTable(joinTableName, type.id.name, sqlDataType(type.id), idPK, false);
						
						
						if(field.relation == None) {
							sql.alterTableAdd(joinTableName, field.name, sqlDataType(field), false);
						} else {
							boolean columnUnique = (field.relation == OneToOne || field.relation == OneToMany);
							sql.alterTableAdd(joinTableName, sqlFKName(field), sqlDataType(field.type.id), 
									field.type.name, field.type.id.name, columnUnique);
						}
						
					}
					
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
			Connection conn = DriverManager.getConnection(config.database.url + "/" + config.database.name, config.database.user, config.database.password);
			conn.setAutoCommit(false);
			return conn;
		}
		
		
		public Entity create(Entity entity, String entityName) throws SQLException {
			return create(Arrays.asList(entity), entityName).get(0);
		}
		
		public List<Entity> create(List<Entity> entities, String entityName) throws SQLException {
			
			// get entity type
			Type entityType = DaoFactory.this.config.model.types.get(entityName);
			if(entityType == null) {
				throw new IllegalArgumentException("Unknown entity type: " + entityName + ".");
			}
			
			
			
			// create row with fields in the main table
			Sql sql = new Sql(conn);
			List<Entity> createdEntities = new ArrayList<>();
			for(Entity entity : entities) {
				List<String> columns = new ArrayList<>();
				List<String> columnTypes = new ArrayList<>();
				List<Object> columnValues = new ArrayList<>();
				for(String fieldName : entity.keySet()) {
					Field field = entityType.fields.get(fieldName);
					if(field.relation == Relation.None) {
						columns.add(field.name);
						columnTypes.add(sqlDataType(field));
						columnValues.add(entity.get(field.name));
					}
				}
				int id = sql.insertInto(entityType.name, columns, columnTypes, columnValues);
				createdEntities.add(Entity.asEntity(entityType.id.name, id));
			}
			
			
			return createdEntities;
			
		}
		public Entity read(int id, Selection select) { return null; }
		public Entity update(Entity entity) { return null; }
		public Entity delete(int id, Selection select) { return null; }
		
		
		public void commit() throws SQLException {
			conn.commit();
			conn.close();
			conn = openConnection();
		}
		
		public void reset() throws SQLException {
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
