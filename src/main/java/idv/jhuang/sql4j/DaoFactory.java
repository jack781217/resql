package idv.jhuang.sql4j;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static idv.jhuang.sql4j.Configuration.Field.Relation.ManyToMany;
import static idv.jhuang.sql4j.Configuration.Field.Relation.ManyToOne;
import static idv.jhuang.sql4j.Configuration.Field.Relation.None;
import static idv.jhuang.sql4j.Configuration.Field.Relation.OneToMany;
import static idv.jhuang.sql4j.Configuration.Field.Relation.OneToOne;
import static java.util.Arrays.asList;
import idv.jhuang.sql4j.Configuration.Field;
import idv.jhuang.sql4j.Configuration.Type;
import idv.jhuang.sql4j.exception.EntityNotFoundException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
	
	public Type types(String typeName) {
		return config.model.types.get(typeName);
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
		
		
		/**
		 * @category CRUD
		 */
		public Entity createOrUpdate(String typeName, Entity entity) throws SQLException {
			log.debug("createOrUpdate {}:\n{}", typeName, entity.toString());
			
			List<Entity> createdEntities = createOrUpdate(typeName, asList(entity));
			return createdEntities.isEmpty() ? null : createdEntities.get(0);
		}
		
		/**
		 * @category CRUD
		 */
		public List<Entity> createOrUpdate(String typeName, List<Entity> entities) throws SQLException {
			log.debug("createOrUpdate {}:\n{}", typeName, entities.toString());
			
			Type type = config.model.types.get(typeName);
			checkArgument(type != null, "Undefined type given: %s.", typeName);

			Sql sql = new Sql(conn);
			List<Entity> createdEntities = new ArrayList<>();
			for(Entity entity : entities) {
				
				Entity createdEntity = new Entity();
				
				Object id = entity.get(type.id.name);
				if(id != null) {
					if(sql.selectExist(type.name, type.id.name, sqlDataType(type.id), id) != 1) {
						throw new EntityNotFoundException("%s[%d]", type.name, id);
					}
				} else {
					id = sql.insertInto(type.name, asList(), asList(), asList(asList())).get(0);
				}
				
				createdEntity.set(type.id.name, id);
				
				entity.remove(type.id.name);
				if(entity.isEmpty()) {
					createdEntities.add(createdEntity);
					continue;
				}
				
				// create row with fields in the main table
				List<String> columns = new ArrayList<>();
				List<String> columnTypes = new ArrayList<>();
				List<Object> columnValues = new ArrayList<>();
				
				
				for(String fieldName : entity.keySet()) {
					Field field = type.fields.get(fieldName);
					
					// master fields
					if(field.master) {
						if(!field.sparse) {
							if(field.relation == None) {
								Object value = entity.get(field.name);
								columns.add(field.name);
								columnTypes.add(sqlDataType(field));
								columnValues.add(value);
							} else if(field.relation == OneToOne || field.relation == ManyToOne) {
								Entity childEntity = createOrUpdate(field.type.name, entity.get(field.name));
								Object childId = childEntity.get(field.type.id.name);
								columns.add(sqlFKName(field));
								columnTypes.add(sqlDataType(field.type.id));
								columnValues.add(childId);
								createdEntity.put(field.name, childEntity);
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
											asList(type.id.name, field.name), 
											asList(sqlDataType(type.id), sqlDataType(field)), 
											asList(asList(id, value)));
								}
							} else {
								List<Entity> children = (field.relation == OneToOne || field.relation == ManyToOne) ?
										createOrUpdate(field.type.name, asList((Entity)entity.get(field.name))) :	
										createOrUpdate(field.type.name, (List<Entity>)entity.get(field.name));
								
								String table = sqlJoinTableName(type, field);
								sql.deleteFrom(table, type.id.name, sqlDataType(type.id), id);
								List<List<Object>> valuess = new ArrayList<>();
								for(Entity child : children)
									valuess.add(asList((Object)id, child.get(field.type.id.name)));
								sql.insertInto(table, 
										asList(type.id.name, sqlFKName(field)), 
										asList(sqlDataType(type.id), sqlDataType(field.type.id)), 
										valuess);
								
								if(field.relation == OneToOne || field.relation == ManyToOne) {
									createdEntity.put(field.name, children.get(0));
								} else {
									createdEntity.put(field.name, children);
								}
							}
						}
						
					// slave fields
					} else {
						List<Entity> children = (field.relation == OneToOne || field.relation == ManyToOne) ?
								createOrUpdate(field.type.name, asList((Entity)entity.get(field.name))) :
								createOrUpdate(field.type.name, (List<Entity>)entity.get(field.name));
						if(!field.sparse) {
							checkState(field.relation != None && field.relation != ManyToOne && field.relation != ManyToMany,
									"Invalid relation for sparse field %s: %s", field.name, field.relation);
							
							for(Entity child : children) {
								sql.updateSet(field.type.name, 
										sqlFKName(field.remote), sqlDataType(type.id), id, 
										field.type.id.name, sqlDataType(field.type.id), child.get(field.type.id.name));
							}
							
							createdEntity.put(field.name, (field.relation == OneToOne) ?
									children.get(0) : children);
							
						} else {
							String table = sqlJoinTableName(field.type, field.remote);
							sql.deleteFrom(table, sqlFKName(field.remote), sqlDataType(type.id), id);
							List<List<Object>> valuess = new ArrayList<>();
							for(Entity child : children)
								valuess.add(asList(child.get(field.type.id.name), id));
							
							sql.insertInto(table, 
									asList(field.type.id.name, sqlFKName(field.remote)), 
									asList(sqlDataType(field.type.id), sqlDataType(type.id)), 
									valuess);
							
							createdEntity.put(field.name, (field.relation == OneToOne || field.relation == ManyToOne) ?
									children.get(0) : children);
						}
								
						
					}
					
				}
				sql.updateSet(type.name, columns, columnTypes, columnValues, type.id.name, sqlDataType(type.id), id);
				
				
				
				createdEntities.add(createdEntity);
			}
			
			
			
			
			return createdEntities;
			
		}

		
		public Entity read(String typeName, Object id, Map<String, Object> selectMap) throws SQLException {
			List<Entity> entities = read(typeName, asList(id), selectMap);
			return entities.isEmpty() ? null : entities.get(0);
		}
		
		public List<Entity> read(String typeName, List<Object> ids, Map<String, Object> selectMap) throws SQLException { 
			log.debug("read {}{}: {}", typeName, ids, selectMap);
			
			Type type = config.model.types.get(typeName);
			checkArgument(type != null, "Undefined type given: %s.", typeName);
			
			if(ids.isEmpty())
				return Arrays.asList();
			
			//set default param if nto provided 
			if(ids == null)
				ids = new ArrayList<>();
			
			
			
			Entity template = new Entity();
			for(String fieldName : selectMap.keySet())
				template.put(fieldName, null);
				
			
			
			List<String> columns = new ArrayList<>();
			List<String> types = new ArrayList<>();
			columns.add(type.id.name);
			types.add(sqlDataType(type.id));
			for(String fieldName : selectMap.keySet()) {
				//log.info("check {}.{}", type.name, fieldName);
				Field field = type.fields.get(fieldName);
				
				
				if(fieldName.equals(type.id.name)) {
					columns.add(fieldName);
					types.add(sqlDataType(type.id));
				} else if(field.relation == None && !field.sparse) {
					columns.add(field.name);
					types.add(sqlDataType(field));
				} else if(field.master && (field.relation == OneToOne || field.relation == ManyToOne) && !field.sparse) {
					columns.add(sqlFKName(field));
					types.add(sqlDataType(field.type.id));
				}
				
			}
			
			
			
			
			Sql sql = new Sql(conn);
			List<List<Object>> rows = sql.selectFrom(type.name, 
					columns, types, 
					asList(type.id.name),
					asList(sqlDataType(type.id)),
					asList("OR"),
					asList(Collections.nCopies(ids.size(), "=")),
					asList(ids));
			
			List<Entity> entities = new ArrayList<>();
			
			for(List<Object> row : rows) {
				Entity entity = new Entity(template);
				int idx = 0;
				
				Object id = row.get(idx);
				idx++;
				
				for(String fieldName : selectMap.keySet()) {
					//log.debug(fieldName);
					Field field = type.fields.get(fieldName);
					
					if(fieldName.equals(type.id.name)) {
						entity.put(fieldName, row.get(idx));
						idx++;
					} else if(field.relation == None && !field.sparse) {
						entity.put(field.name, row.get(idx));
						idx++;
					} else if(field.relation == None && field.sparse) {
						List<List<Object>> childRows = sql.selectFrom(sqlJoinTableName(type, field), 
								asList(field.name), asList(sqlDataType(field)),
								asList(type.id.name), asList(sqlDataType(type.id)), asList("OR"), 
								asList(asList("=")), asList(asList(id)));
						if(!childRows.isEmpty()) {
							entity.put(field.name, childRows.get(0).get(0));
						}
					} else if(field.master && (field.relation == OneToOne || field.relation == ManyToOne)) {
						
						Object childId = null; 
						if(!field.sparse) {
							childId = row.get(idx);
							idx++;
						} else {
							List<List<Object>> mappingRows = sql.selectFrom(sqlJoinTableName(type, field),
									asList(sqlFKName(field)), asList(sqlDataType(field.type.id)),
									asList(type.id.name), asList(sqlDataType(type.id)), asList("OR"),
									asList(asList("=")), asList(asList(id)));
							if(!mappingRows.isEmpty()) {
								childId = mappingRows.get(0).get(0);
							}
						}

						//log.info(field.name);
						
						if(childId != null) {
							Entity childEntity = read(field.type.name, asList(childId), 
									(Map<String, Object>)selectMap.get(field.name)).get(0);
							entity.put(field.name, childEntity);
							
						}
						
					} else if(field.master && (field.relation == OneToMany || field.relation == ManyToMany)) {
						
						List<List<Object>> mappingRows = sql.selectFrom(sqlJoinTableName(type, field),
								asList(sqlFKName(field)), asList(sqlDataType(field.type.id)),
								asList(type.id.name), asList(sqlDataType(type.id)), asList("OR"),
								asList(asList("=")), asList(asList(id)));
						
						List<Object> childIds = new ArrayList<>();
						for(List<Object> mappingRow : mappingRows) {
							childIds.add(mappingRow.get(0));
						}
						
						List<Entity> childEntities = read(field.type.name, childIds, 
								(Map<String, Object>)selectMap.get(field.name));
						entity.put(field.name, childEntities);
						
					} else if(!field.master && (field.relation == OneToOne || field.relation == OneToMany || field.relation == ManyToOne || field.relation == ManyToMany)) {
						List<Object> childIds = new ArrayList<>();
						
						if((field.relation == OneToOne || field.relation == OneToMany) && !field.remote.sparse) {
							List<List<Object>> mappingRows = sql.selectFrom(field.type.name, 
									asList(field.type.id.name), asList(sqlDataType(field.type.id)),
									asList(sqlFKName(field.remote)), asList(sqlDataType(type.id)), asList("OR"),
									asList(asList("=")), asList(asList(id)));
							
							
							for(List<Object> mappingRow : mappingRows) {
								childIds.add(mappingRow.get(0));
							}
							
						} else {
						
							List<List<Object>> mappingRows = sql.selectFrom(sqlJoinTableName(field.type, field.remote),
									asList(field.type.id.name), asList(sqlDataType(field.type.id)),
									asList(sqlFKName(field.remote)), asList(sqlDataType(type.id)), asList("OR"),
									asList(asList("=")), asList(asList(id)));
							for(List<Object> mappingRow : mappingRows) {
								childIds.add(mappingRow.get(0));
							}
						}
						
						List<Entity> childEntities = read(field.type.name, childIds, 
								(Map<String, Object>)selectMap.get(field.name));
						if(field.relation == OneToOne ||field.relation == ManyToOne) {
							if(!childEntities.isEmpty())
								entity.put(field.name, childEntities.get(0));
						} else if(field.relation == ManyToMany || field.relation == OneToMany) {
							entity.put(field.name, childEntities);
						}
					}
				}
				entities.add(entity);
			}
			
			return entities; 
		}
		
		
		public Entity delete(int id, String select) { return null; }
		
		
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
		
		public Map<String, Object> parseSelection(String selectStr, Type type) {
			Map<String, Object> selectMap = new LinkedHashMap<>();
			parseSelection(selectMap, selectStr, type, 1);
			return selectMap;
		}
		
		private int parseSelection(Map<String, Object> selectMap, String selectStr, Type type, int start) {
			int i = start;
			int j = i;
			int k;
			while(j < selectStr.length()) {
				switch(selectStr.charAt(j)) {
				case ',':
					if(i != j)
						selectMap.put(selectStr.substring(i, j), "");
					i = j + 1;
					j = i;
					break;
				case '[':
					Map<String, Object> childSelectMap = new LinkedHashMap<>();
					k = parseSelection(childSelectMap, selectStr, type, j + 1);
					selectMap.put(selectStr.substring(i, j), childSelectMap);
					i = k + 1;
					j = i;
					break;
				case ']':
					if(i != j)
						selectMap.put(selectStr.substring(i, j), "");
					return j;
				default:
					j++;
				}
			}
			return j;
		}
	}
	
	
	
	
	
	
	
}
