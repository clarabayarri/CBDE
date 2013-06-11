import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;


public class Neo4jNormalized {
	
	private static DataInserter inserter = new DataInserter();
	

    public static void main( final String[] args )
    {
//    	inserter.removeData();
//    	inserter.shutDown();
    	
    	inserter.initialInsert();
    }
}
