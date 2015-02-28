package idv.jhuang.sql4j;

import static com.google.common.base.Preconditions.checkState;

import java.io.InputStream;
import java.sql.Driver;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;


public class Configuration {
	private static final Logger log = LogManager.getLogger(Configuration.class);
	
	
	
	@SuppressWarnings("unchecked")
	public static Configuration parseConfiguration(InputStream stream) throws ClassNotFoundException {
		XStream xstream = new XStream();
		xstream.processAnnotations(FieldXmlNode.class);
		xstream.processAnnotations(TypeXmlNode.class);
		xstream.processAnnotations(ModelXmlNode.class);
		xstream.processAnnotations(DatabaseXmlNode.class);
		xstream.processAnnotations(ConfigurationXmlNode.class);
		
		Configuration configuration = new Configuration();
		
		ConfigurationXmlNode configurationNode = (ConfigurationXmlNode) xstream.fromXML(stream);
		DatabaseXmlNode databaseNode = configurationNode.database;
		configuration.database = new Database();
		configuration.database.driver = (Class<Driver>) Class.forName(databaseNode.driver);
		configuration.database.user = databaseNode.user;
		configuration.database.password = databaseNode.password;
		configuration.database.url = databaseNode.url;
		configuration.database.name = databaseNode.name;
		
		//ModelXmlNode modelNode = (ModelXmlNode) xstream.fromXML(stream);
		ModelXmlNode modelNode = configurationNode.model;
		Model model = new Model();
		model.types = new LinkedHashMap<>();
		configuration.model = model;
				
		// first pass: create Models
		for(TypeXmlNode typeNode : modelNode.types) {
			checkState(typeNode.name != null && !typeNode.name.isEmpty(),
					"Invalid type name: %s. Model name is mandatory.", 
					typeNode.name);
			checkState(!Type.DATA_TYPES.containsKey(typeNode.name),
					"Invalid type name :%s. Model name must not be one of the data types %s.",
					typeNode.name, Type.DATA_TYPES.keySet());
			
			Type type = new Type();
			type.name = typeNode.name;
			type.fields = new LinkedHashMap<>();
			
			model.types.put(typeNode.name, type);
		}
		
		// second pass: create Fields
		for(TypeXmlNode typeNode : modelNode.types) {
			Type type = model.types.get(typeNode.name);
			
			Field idField = new Field();
			idField.name = "id";
			idField.type = Type.INT;
			type.id = idField;
			
			
			
			for(FieldXmlNode fieldNode : typeNode.fields) {
				
				Field field = new Field();
				
				// name 
				checkState(fieldNode.name != null && !fieldNode.name.trim().isEmpty(), 
						"Invalid Field.name in Model %s: %s. Field.name is mandatory.", type.name, fieldNode.name);
				field.name = fieldNode.name;
				
				// type
				if(Type.DATA_TYPES.containsKey(fieldNode.type)) 
					field.type = Type.DATA_TYPES.get(fieldNode.type);
				else if(model.types.containsKey(fieldNode.type))
					field.type = model.types.get(fieldNode.type);
				else
					checkState(false, "Invalid Field.type in Model %s: %s. Field.type must be either one of the data types (%s) or one of the user defined Models (%s).",
							type.name, fieldNode.type, Type.DATA_TYPES, model.types.keySet());

				// relation
				if(Type.DATA_TYPES.containsKey(fieldNode.type)) {
					checkState(fieldNode.relation == null, 
							"Invalid Field.relation in Model %s: %s. Field.relation must be null if Field.type is one of the data types.",
							type.name, fieldNode.relation);
					field.relation = Field.Relation.None;
				} else {
					checkState(Field.Relation.isValid(fieldNode.relation), 
							"Invalid property 'relation' for %s.%s: %s. Property 'relation' must be one of %s if 'type' is one of the user defined Model.",
							type.name, fieldNode.name, fieldNode.relation, Arrays.toString(Field.Relation.values()));
					field.relation = Field.Relation.valueOf(fieldNode.relation);
				}
				
				// remote
				if(field.relation == Field.Relation.None) {
					checkState(fieldNode.remote == null,
							"Invalid property 'remote' in Field %s.%s: %s. Property 'remote' must be null if 'relation' is null.",
							type.name, fieldNode.name, fieldNode.remote);
				} else {
					checkState(fieldNode.remote == null || !fieldNode.remote.trim().isEmpty(),
							"Invalid property 'remote' in Field %s.%s: %s. Property 'remote' must be null or non-empty if 'relation' is not null.",
							type.name, fieldNode.name, field.remote);
				}
				field.remote = fieldNode.remote;
					
				// sparse
				
				switch(field.relation) {
				case None:
				case OneToOne:
				case ManyToOne:
					checkState(fieldNode.sparse == null || fieldNode.sparse.equals("true") || fieldNode.sparse.equals("false"),
						"Invalid property 'sparse' for %s.%s: %s. Property 'sparse' msut be either null, 'true', or 'false'.",
						type.name, fieldNode.name, fieldNode.sparse);
					field.sparse = Boolean.parseBoolean(fieldNode.sparse);
					break;
				case OneToMany:
				case ManyToMany:
					checkState(fieldNode.sparse == null, "Property 'sparse' cannot be specified for OneToMany or ManyToMany relations.");
					field.sparse = true;
					break;
				}
				
				
				// values
				if(field.type == Type.ENUM) {
					checkState(fieldNode.values != null && !fieldNode.values.trim().isEmpty(),
							"Invalid property 'values' for %s.%s: %s. Property 'values' must be non-empty if 'type' is enum.",
							type.name, fieldNode.name, fieldNode.values);
					field.values = Arrays.asList(fieldNode.values.split(","));
				} else {
					checkState(fieldNode.values == null,
							"Invalid property 'values' for %s.%s: %s. Property 'values' must be null if 'type' is not enum.",
							type.name, fieldNode.name, fieldNode.values);
					field.values = null;
				}
				
				// master
				field.master = true;
				
				
				
				type.fields.put(field.name, field);
			}
			

			
			model.types.put(type.name, type);
		}
		
		// third pass: create slave fields
		for(TypeXmlNode typeNode : modelNode.types) {
			Type type = model.types.get(typeNode.name);
			for(FieldXmlNode fieldNode : typeNode.fields) {
				Field field = type.fields.get(fieldNode.name);
				
				if(field.remote != null) {
					Type remoteModel = field.type;
					Field remoteField = new Field();
					remoteField.name = field.remote;
					remoteField.type = type;
					remoteField.relation = Field.Relation.opposite(field.relation);
					remoteField.remote = field.name;
					remoteField.values = null;
					remoteField.sparse = true;
					remoteField.master = false;
					
					checkState(!remoteModel.fields.containsKey(remoteField.name),
							"Cannot create remote field %s.%s from %s.%s: field name alreayd used.",
							remoteModel.name, remoteField.name, type.name, field.name);
					
					remoteModel.fields.put(remoteField.name, remoteField);
				}
			}
		}
		
		
		return configuration;
	}
	
	
	public Database database;
	public Model model;
	
	//================================================================================
	//	Java POJO
	//================================================================================
	
	public static class Database {
		public Class<Driver> driver;
		public String url;
		public String user;
		public String password;
		public String name;
		
		private Database() {
			
		}
		
		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this)
					.add("driver", driver)
					.add("url", url)
					.add("username", user)
					.add("password", password)
					.add("name", name)
					.toString();
		}
	}
	
	/**
	 * @category Java POJO
	 */
	public static class Model {
		public Map<String, Type> types;
		
		private Model() { 
			
		}
		
		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this)
					.add("types", types)
					.toString();
		}
		
	}

	/**
	 * @category Java POJO
	 */
	public static class Type {
		public static final Type INT = new Type("int");
		public static final Type STRING = new Type("string");
		public static final Type DOUBLE = new Type("double");
		public static final Type BOOLEAN = new Type("boolean");
		public static final Type DATE = new Type("date");
		public static final Type ENUM = new Type("enum");
		public static final Map<String, Type> DATA_TYPES = ImmutableMap.<String, Type>builder()
				.put(INT.name, INT)
				.put(STRING.name, STRING)
				.put(DOUBLE.name, DOUBLE)
				.put(BOOLEAN.name, BOOLEAN)
				.put(DATE.name, DATE)
				.put(ENUM.name, ENUM)
			    .build();

		public String name;
		public Field id;
		public Map<String, Field> fields;
		
		private Type() { 
			
		}
		
		private Type(String name) {
			this.name = name;
		}
		
		
		public String toString() {
			return MoreObjects.toStringHelper(this)
					.add("name", name)
					.add("id", id)
					.add("fields", fields)
					.toString();
		}
	}
	
	/**
	 * @category Java POJO
	 */
	public static class Field {
		public static enum Relation {
			None, OneToOne, OneToMany, ManyToOne, ManyToMany;
			public static boolean isValid(String value) {
				return value != null && (
						value.equals(OneToOne.toString()) ||
						value.equals(OneToMany.toString()) ||
						value.equals(ManyToOne.toString()) ||
						value.equals(ManyToMany.toString()) );
			}
			public static Relation opposite(Relation relation) {
				switch(relation) {
				case OneToMany:	return ManyToOne;
				case ManyToOne:	return OneToMany;
				default:	return relation;
				}
			}
		}
		
		public String name;
		public Type type;
		public Relation relation;
		public String remote;
		public boolean sparse;
		public boolean master;
		public List<String> values;
		
		private Field() {
			
		}
		



		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this)
		       .add("name", name)
		       .add("type", type.name)
		       .add("relation", relation)
		       .add("master", master)
		       .add("remote", remote)
		       .add("sparse", sparse)
		       .add("values", values)
		       .toString();
		}
	}
	
	
	
	//================================================================================
	//	XML POJO
	//================================================================================
	
	@XStreamAlias("configuration")
	private static class ConfigurationXmlNode {
		private DatabaseXmlNode database;
		private ModelXmlNode model;
	}
	
	@XStreamAlias("database")
	private static class DatabaseXmlNode {
		@XStreamAsAttribute public String driver;
		@XStreamAsAttribute public String url;
		@XStreamAsAttribute public String user;
		@XStreamAsAttribute public String password;
		@XStreamAsAttribute public String name;
	}
	
	/**
	 * @category XML POJO
	 */
	@XStreamAlias("model")
	private static class ModelXmlNode {
		@XStreamImplicit private List<TypeXmlNode> types;
		@Override public String toString() {
			return MoreObjects.toStringHelper(this)
					.add("types", types)
					.toString();
		}
	}
	/**
	 * @category XML POJO
	 */
	@XStreamAlias("type")
	private static class TypeXmlNode {
		@XStreamAsAttribute private String name;
		@XStreamImplicit private List<FieldXmlNode> fields;
		@Override public String toString() {
			if(name == null)
				System.out.println("name is null");
			else if(name.isEmpty())
				System.out.println("name is empty");
			return MoreObjects.toStringHelper(this)
					.add("name", name)
					.add("fields", fields)
					.toString();
		}
	}
	/**
	 * @category XML POJO
	 */
	@XStreamAlias("field")
	private static class FieldXmlNode {
		@XStreamAsAttribute private String name;
		@XStreamAsAttribute private String type;
		@XStreamAsAttribute private String relation;
		@XStreamAsAttribute private String remote;
		@XStreamAsAttribute private String sparse;
		@XStreamAsAttribute private String values;
		@Override public String toString() {
			return MoreObjects.toStringHelper(this)
					.add("name", name)
					.add("type", type)
					.add("relation", relation)
					.add("remote", remote)
					.add("sparse", sparse)
					.add("values", values)
					.toString();
		}
	}
	
	
	
}
