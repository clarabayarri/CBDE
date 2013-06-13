

public class Neo4jNormalized {
	
	private static DataInserter inserter = new DataInserter();
	

    public static void main( final String[] args )
    {
//    	inserter.removeData();
//    	inserter.shutDown();
    	
    	inserter.initialInsert();
    }
}
