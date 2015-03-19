package idv.jhuang.sql4j.controller;

import idv.jhuang.sql4j.DaoFactory.Dao;
import idv.jhuang.sql4j.Entity;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.type.TypeReference;

@Path("/entity")
public class EntityController extends Controller {
	
	public EntityController() throws ClassNotFoundException {
		super();
	}
	
	public EntityController(InputStream configFileStream) throws ClassNotFoundException {
		super(configFileStream);
	}



	
	@POST @Path("/{entityName}")
	@Consumes(MediaType.APPLICATION_JSON) @Produces(MediaType.APPLICATION_JSON)
	public Response create(
			@PathParam("entityName") String entityName,
			String entityJson) {
		log.info("POST /{}\n{}", entityName, entityJson);
		String res;
		
		try(Dao dao = factory.createDao()) {
			List<Entity> entities = mapper.readValue(entityJson, List.class);
			
			List<Entity> createdEntities = dao.createOrUpdate(factory.types(entityName), entities);
			
			dao.commit();
			
			res = mapper.writeValueAsString(createdEntities);
			
			
		} catch(Exception e) {
			return serverError("", e);
		}
		
		
		return Response.ok(res).build();
	}
}
