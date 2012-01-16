@Grab('org.neo4j:neo4j:1.6.M03')


@Grab('mysql:mysql-connector-java:5.1.6') import groovy.sql.Sql
import org.neo4j.graphdb.index.BatchInserterIndex
import org.neo4j.graphdb.index.BatchInserterIndexProvider
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider
import org.neo4j.kernel.impl.batchinsert.BatchInserter
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl
import org.neo4j.kernel.impl.util.StringLogger

def sql = Sql.newInstance("jdbc:mysql://localhost/twitter1", "twitter", "twitter", "com.mysql.jdbc.Driver");

new File("/tmp/tweets").deleteDir();

def props = [:]
props[StringLogger.class] = StringLogger.SYSTEM;
BatchInserter inserter = new BatchInserterImpl( "/tmp/tweets",props);
BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider( inserter );

def usersCache = [:]

BatchInserterIndex users = indexProvider.nodeIndex( "users", [type:'exact'] );
users.setCacheCapacity( "name", 100000 );
int k=0;
(1..4).each { i->
sql.eachRow("select * from twitter" + i + ".users", {
  k++;
  def user = [type: 'user', name: it.name, screenName: it.screen_name, id: it.id];
  long node = inserter.createNode( user );
  usersCache[it.id] = node;
  users.add( node, user );
    
  if (k%1000 == 0) users.flush();
})
  println "finished users ${i}"
}
users.flush();

for (i in 1..4) {
    sql.eachRow("select * from twitter" + i + ".followers", {
        if (it.user_id > 0 && it.follower_id>0)
        inserter.createRelationship(usersCache[it.follower_id], usersCache[it.user_id], RelTypes.FOLLOWS, [:]);
    });
    println "finished relations ${i}"
}

for (i in 1..4) {
        sql.eachRow("select * from twitter" + i + ".statuses s", {
            if (it.user_id > 0) {
              long status = inserter.createNode([type: 'status', date: it.created_at.time, text: it.text]);
              inserter.createRelationship(usersCache[it.user_id],status, RelTypes.STATUS,[:])
            }
        });
        println "finished timeline ${i}"
}
indexProvider.shutdown()
inserter.shutdown()
