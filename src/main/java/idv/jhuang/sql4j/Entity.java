package idv.jhuang.sql4j;

import idv.jhuang.sql4j.Configuration.Field;
import idv.jhuang.sql4j.Configuration.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public class Entity extends LinkedHashMap<String, Object> {

	public Entity() {
		super();
	}
	public Entity(Map<String, Object> map) {
		super(map);
		for(Map.Entry<String, Object> entry : map.entrySet()) {
			if(entry.getValue() instanceof Map)
				entry.setValue(new Entity((Map<String, Object>)entry.getValue()));
		}
	}
	
	@SuppressWarnings("unchecked")
	public <T> T get(String field) {
		return (T) super.get(field);
	}
	
	public <T> void set(String field, T value) {
		super.put(field, value);
	}
	
	
	
	public static Entity asEntity(Object... objs) {
		Entity entity = new Entity();
		for(int i = 0; i < objs.length; i += 2)
			entity.put((String)objs[i], objs[i + 1]);
		return entity;
	}
	
	public static List<Entity> uncompress(List<Object> entityObjs, Type entityType) {
		List<Entity> entities = new ArrayList<>(entityObjs.size());
		for(int i = 0; i < entityObjs.size(); i++) {
			entities.set(i, uncompress(entityObjs.get(i), entityType));
		}
		return entities;
	}
	
	private static Entity uncompress(Object entityObj, Type entityType) {
		if(entityObj instanceof Integer) {
			return Entity.asEntity(entityType.id.name, entityObj);
			
		} else {
			Entity entity = (Entity)entityObj;
			for(Field field : entityType.fields.values()) {
				switch(field.relation) {
				case OneToOne:
				case ManyToOne:
					Object child = entity.get(field.name);
					entity.set(field.name, uncompress(child, field.type));
					break;
				case OneToMany:
				case ManyToMany:
					List<Object> children = entity.get(field.name);
					entity.set(field.name, uncompress(children, field.type));
					break;
				default:
				}
			}
			return entity;
		}
	}
	
	public static List<Object> compress(List<Entity> entities, Type entityType) {
		List<Object> entityObjs = new ArrayList<>(entities.size());
		for(int i = 0; i < entityObjs.size(); i++) {
			entityObjs.set(i, compress(entities.get(i), entityType));
		}
		return entityObjs;
	}
	private static Object compress(Entity entity, Type entityType) {
		if(entity.size() == 1 && entity.containsKey(entityType.id.name)) {
			return entity.get(entityType.id.name);
			
		} else {
			for(Field field : entityType.fields.values()) {
				switch(field.relation) {
				case OneToOne:
				case ManyToOne:
					Entity child = entity.get(field.name);
					entity.set(field.name, compress(child, field.type));
					break;
				case OneToMany:
				case ManyToMany:
					List<Entity> children = entity.get(field.name);
					entity.set(field.name, compress(children, field.type));
					break;
				default:
				}
			}
			return entity;
		}
	}

}