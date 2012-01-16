@Grab('org.neo4j:neo4j:1.6.M03')

import org.neo4j.kernel.EmbeddedGraphDatabase
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.index.Index
import org.neo4j.graphdb.Node

GraphDatabaseService graphDb = new EmbeddedGraphDatabase("/tmp/tweets");
Index<Node> nodeIndex = graphDb.index().forNodes("users");

args = //['insert', 'tristian_yundt','tralalala']
       //['wall', 'tristian_yundt']
       ['statuses', 'tristian_yundt']
       //["users","B"]

String operation = args[0]

def getUser = {
    nodeIndex.get("screenName", args[1]).single;
}

def operations = [:]
operations.insert = {->
    String text = args[2]
    Node user = getUser();
    Transaction tx = graphDb.beginTx();
    try {
        Node status = graphDb.createNode();
        status.setProperty("type", "status");
        status.setProperty('date', new Date().time)
        status.setProperty('text', text);
        user.createRelationshipTo(status, RelTypes.STATUS);
        tx.success();
    }
    finally {
        tx.finish();
    }
}

operations.users = {
    println "users ${args[1]}"
    nodeIndex.query("name:${args[1]}*").each {
        Node node -> println "${node.getProperty("name")}: ${node.getProperty("screenName")}"
    }
}
operations.wall = {->
    Node user = getUser();
    user.getRelationships(RelTypes.FOLLOWS).collect { it.endNode }.
            collect { following ->
                following.getRelationships(RelTypes.STATUS).collect {
                    statusRel ->
                    status = statusRel.endNode
                    [date: status.getProperty('date'), text: status.getProperty('text'), name: following.getProperty("name")]
                }
            }.flatten().sort { it.date }.each {
        println "${it.name}: ${new Date(it.date)}: ${it.text}"
    };
}

operations.statuses = {->
    Node user = getUser();
    user.getRelationships(RelTypes.STATUS).collect { it.endNode }.sort { it.getProperty('date') }.each {
        println "${new Date(it.getProperty('date'))}: ${it.getProperty('text')}"
    };
}
operations[operation]();

