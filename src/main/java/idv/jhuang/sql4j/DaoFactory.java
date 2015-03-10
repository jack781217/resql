package idv.jhuang.sql4j;

import static com.google.common.base.Preconditions.checkState;
import static idv.jhuang.sql4j.Configuration.Field.Relation.ManyToMany;
import static idv.jhuang.sql4j.Configuration.Field.Relation.ManyToOne;
import static idv.jhuang.sql4j.Configuration.Field.Relation.None;
import static idv.jhuang.sql4j.Configuration.Field.Relation.OneToMany;
import static idv.jhuang.sql4j.Configuration.Field.Relation.OneToOne;
import idv.jhuang.sql4j.Configuration.Field;
import idv.jhuang.sql4j.Configuration.Type;
import idv.jhuang.sql4j.exception.EntityNotFoundException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
		
		
		public Entity createOrUpdate(String entityName, Entity entity) throws SQLException {
			List<Entity> createdEntities = createOrUpdate(entityName, Arrays.asList(entity));
			return createdEntities.isEmpty() ? null : createdEntities.get(0);
		}
		
		public List<Entity> createOrUpdate(String entityName, List<Entity> entities) throws SQLException {
			
			// get entity type
			Type type = DaoFactory.this.config.model.types.get(entityName);
			if(type == null) {
				throw new IllegalArgumentException("Unknown entity type: " + entityName + ".");
			}
			
			

			Sql sql = new Sql(conn);
			List<Entity> createdEntities = new ArrayList<>();
			for(Entity entity : entities) {
				
				Object id = entity.get(type.id.name);
				if(id != null) {
					if(sql.selectExist(type.name, type.id.name, sqlDataType(type.id), id) != 1) {
						throw new EntityNotFoundException("%s[%d]", type.name, id);
					}
				} else {
					id = sql.insertInto(type.name, Arrays.asList(), Arrays.asList(), Arrays.asList(Arrays.asList())).get(0);
				}
				
				
				
				// create row with fields in the main table
				List<String> columns = new ArrayList<>();
				List<String> columnTypes = new ArrayList<>();
				List<Object> columnValues = new ArrayList<>();
				for(String fieldName : entity.keySet()) {
					Field field = type.fields.get(fieldName);
					
					if(field.master) {
						if(!field.sparse) {
							if(field.relation == None) {
								Object value = entity.get(field.name);
								columns.add(field.name);
								columnTypes.add(sqlDataType(field));
								columnValues.add(value);
							} else if(field.relation == OneToOne || field.relation == ManyToOne) {
								Object childId = createOrUpdate(field.type.name, entity.get(field.name)).get(field.type.id.name);
								columns.add(sqlFKName(field));
								columnTypes.add(sqlDataType(field.type.id));
								columnValues.add(childId);
							} else {
								checkState(false, "Sparse field %s has relation %s.", field.name, field.relation);
							}
							
						} else {
							if(field.relation == None) {
								Object value = entity.get(field.name);
								String table = sqlJoinTableName(type, field);
								sql.deleteFrom(table, type.id.name, sqlDataType(type.id), id);
								if(value != null) {
									sql.insertInto(table, 
											Arrays.asList(type.id.name, field.name), 
											Arrays.asList(sqlDataType(type.id), sqlDataType(field)), 
											Arrays.asList(Arrays.asList(id, value)));
								}
							/*} else if(field.relation == OneToOne || field.relation == ManyToOne) {
								Object childId = createOrUpdate(field.type.name, entity.get(field.name)).get(field.type.id.name);
								String table = sqlJoinTableName(type, field);
								sql.deleteFrom(table, type.id.name, sqlDataType(type.id), id);
								if(childId != null) {
									sql.insertInto(table, 
											Arrays.asList(type.id.name, sqlFKName(field)), 
											Arrays.asList(sqlDataType(type.id), sqlDataType(field.type.id)), 
											Arrays.asList(Arrays.asList(id, childId)));
								}*/
							} else {
								String table = sqlJoinTableName(type, field);
								sql.deleteFrom(table, type.id.name, sqlDataType(type.id), id);
								
								List<Entity> children = (field.relation == OneToOne || field.relation == ManyToOne) ?
										createOrUpdate(field.type.name, Arrays.asList((Entity)entity.get(field.name))) :	
										createOrUpdate(field.type.name, (List<Entity>)entity.get(field.name));
								List<List<Object>> valuess = new ArrayList<>();
								for(Entity child : children) {
									valuess.add(Arrays.asList((Object)id, child.get(field.type.id.name)));
								}
								sql.insertInto(table, 
										Arrays.asList(type.id.name, sqlFKName(field)), 
										Arrays.asList(sqlDataType(type.id), sqlDataType(field.type.id)), 
										valuess);
							}
						}
						/*}
					
					if(field.master) {
						switch(field.relation) {
						case None:
							Object value = entity.get(field.name);
							if(!field.sparse) {
								columns.add(field.name);
								columnTypes.add(sqlDataType(field));
								columnValues.add(value);
							} else {
								String table = sqlJoinTableName(type, field);
								sql.deleteFrom(table, type.id.name, sqlDataType(type.id), id);
								if(value != null) {
									sql.insertInto(table, 
											Arrays.asList(type.id.name, field.name), 
											Arrays.asList(sqlDataType(type.id), sqlDataType(field)), 
											Arrays.asList(Arrays.asList(id, value)));
								}
							}
							break;
						
						case OneToOne:
						case ManyToOne:
							Object childId = createOrUpdate(field.type.name, entity.get(field.name)).get(field.type.id.name);
							if(!field.sparse) {
								columns.add(sqlFKName(field));
								columnTypes.add(sqlDataType(field.type.id));
								columnValues.add(childId);
							
							} else {
								String table = sqlJoinTableName(type, field);
								sql.deleteFrom(table, type.id.name, sqlDataType(type.id), id);
								if(childId != null) {
									sql.insertInto(table, 
											Arrays.asList(type.id.name, sqlFKName(field)), 
											Arrays.asList(sqlDataType(type.id), sqlDataType(field.type.id)), 
											Arrays.asList(Arrays.asList(id, childId)));
								}
							}
							break;
						
						case OneToMany:
						case ManyToMany:
							String table = sqlJoinTableName(type, field);
							sql.deleteFrom(table, type.id.name, sqlDataType(type.id), id);
							
							List<Entity> children = entity.get(field.name);
							children = createOrUpdate(field.type.name, children);
							List<List<Object>> valuess = new ArrayList<>();
							for(Entity child : children) {
								valuess.add(Arrays.asList((Object)id, child.get(field.type.id.name)));
							}
							sql.insertInto(table, 
									Arrays.asList(type.id.name, sqlFKName(field)), 
									Arrays.asList(sqlDataType(type.id), sqlDataType(field.type.id)), 
									valuess);
							break;
						}
					*/
					} else {
						List<Entity> children = (field.relation == OneToOne || field.relation == ManyToOne) ?
								createOrUpdate(field.type.name, Arrays.asList((Entity)entity.get(field.name))) :
								createOrUpdate(field.type.name, (List<Entity>)entity.get(field.name));
						if(!field.sparse) {
							checkState(field.relation != None && field.relation != ManyToOne && field.relation != ManyToMany,
									"Invalid relation for sparse field %s: %s", field.name, field.relation);
							for(Entity child : children) {
								sql.updateSet(field.type.name, 
										sqlFKName(field.remote), sqlDataType(type.id), id, 
										field.type.id.name, sqlDataType(field.type.id), child.get(field.type.id.name));
							}
						} else {
							String table = sqlJoinTableName(field.type, field.remote);
							sql.deleteFrom(table, sqlFKName(field.remote), sqlDataType(type.id), id);
							List<List<Object>> valuess = new ArrayList<>();
							for(Entity child : children)
								valuess.add(Arrays.asList(child.get(field.type.id.name), id));
							
							sql.insertInto(table, 
									Arrays.asList(field.type.id.name, sqlFKName(field.remote)), 
									Arrays.asList(sqlDataType(field.type.id), sqlDataType(type.id)), 
									valuess);
						}
								
						
					}
					
				}
				sql.updateSet(type.name, columns, columnTypes, columnValues, type.id.name, sqlDataType(type.id), id);
				
				
				
				createdEntities.add(Entity.asEntity(type.id.name, id));
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
