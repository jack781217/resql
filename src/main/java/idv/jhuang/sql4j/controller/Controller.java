package idv.jhuang.sql4j.controller;

import idv.jhuang.sql4j.Configuration;
import idv.jhuang.sql4j.DaoFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class Controller {
	protected DaoFactory factory;
	protected ObjectMapper mapper;
	protected TypeReference<List<Map<String, Object>>> listMapTypeRef;
	protected final Logger log = LogManager.getLogger(getClass());
	
	
	protected Controller() throws ClassNotFoundException {
		this(Controller.class.getClassLoader().getResourceAsStream("sql4j.xml"));
	}
	protected Controller(InputStream configFileStream) throws ClassNotFoundException {
		factory = new DaoFactory(Configuration.parseConfiguration(configFileStream));
		mapper = new ObjectMapper();
		listMapTypeRef = new TypeReference<List<Map<String, Object>>>(){};
	}
	protected Response serverError(String msg, Exception e) {
		log.error(msg, e);
		return Response.serverError().entity(msg + "\n" + e).build();
	}
	
	@GET @Path("/") 
	@Produces(MediaType.TEXT_PLAIN)
	public Response hello() {
		log.info("GET /");
		return Response.ok(getClass(), MediaType.TEXT_PLAIN).build();
	}
}
