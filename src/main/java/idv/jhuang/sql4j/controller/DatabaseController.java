package idv.jhuang.sql4j.controller;

import idv.jhuang.sql4j.Configuration.Field;
import idv.jhuang.sql4j.Configuration.Field.Relation;
import idv.jhuang.sql4j.Configuration.Type;

import java.io.InputStream;
import java.sql.SQLException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/db")
public class DatabaseController extends Controller {
	
	public DatabaseController() throws ClassNotFoundException {
		super();
	}
	public DatabaseController(InputStream configInputStream) throws ClassNotFoundException {
		super(configInputStream);
	}



	
	
	
	@POST @Path("/reset")
	public Response reset() {
		log.info("POST /reset");
		
		try {
			factory.init();
		} catch(SQLException e) {
			return serverError("Error while resetting database.", e);
		}
		
		return Response.ok("Database resetted.").build();
	}
	
	@GET @Path("/schema")
	public Response schema() {
		log.info("GET /schema");
		
		StringBuffer sb = new StringBuffer();
		
		for(Type type : factory.config.model.types.values()) {
			sb.append(type.name + " {\n");
			
			sb.append(String.format("\t%s: <%s>, \n", type.id.name, type.id.type.name));
			
			for(Field field : type.fields.values()) {
				String fieldStr = "";
				if(field.relation == Relation.None) {
					if(field.type != Type.ENUM) {
						fieldStr = String.format("\t%s: <%s>, \n", field.name, field.type.name);	
					} else {
						fieldStr = String.format("\t%s: <\"%s\">, \n", field.name, String.join("\"|\"", field.values));
					}
				} else if(field.relation == Relation.OneToOne || field.relation == Relation.ManyToOne) {
					fieldStr = String.format("\t%s: <%s>, \n", field.name, field.type.name);
				} else {
					fieldStr = String.format("\t%s: [<%s>, ...], \n", field.name, field.type.name);
				}
				
				sb.append(fieldStr);
			}
			
			sb.setCharAt(sb.length() - 3, ' ');
			sb.append("}\n\n");
		}
		
		
		return Response.ok(sb.toString()).build();
	}
	
	
}
