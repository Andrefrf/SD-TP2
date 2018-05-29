package api.storage;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path(Namenode.PATH)
public interface Namenode {

	static final String PATH="/namenode";
	
	@GET
	@Path("/list/")
	@Produces(MediaType.APPLICATION_JSON)
	List<String> list( @QueryParam("prefix") String prefix);

	@GET
	@Path("/{name}")
	@Produces(MediaType.APPLICATION_JSON)
	List<String> read(@PathParam("name") String name);

	@POST
	@Path("/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	void create(@PathParam("name") String name, List<String> blocks);

	@PUT
	@Path("/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	void update(@PathParam("name") String name, List<String> blocks);

	@DELETE
	@Path("/list/")
	void delete( @QueryParam("prefix") String prefix);

}
