# flink-elasticsearch2-connector

Flink DataSet ElasticSearchOutputFormat create by <https://www.iteblog.com> based on `org.apache.flink#flink-connector-elasticsearch2_2.10#1.1.2`,We can use it in Scala or Java.

# Usage

## pom.xml
```xml
<dependency>
       <groupId>com.iteblog</groupId>
       <artifactId>flink-elasticsearch2-connector</artifactId>
       <version>1.0.1</version>
</dependency>
```

## Using in Scala
```scala
import scala.collection.JavaConversions._
val config = Map("bulk.flush.max.actions" -> "1000", "cluster.name" -> "elasticsearch")
val hosts = "www.iteblog.com"

val transports = hosts.split(",").map(host => new InetSocketAddress(InetAddress.getByName(host), 9300)).toList

val data : DataSet[String] = ....
data.output(new ElasticSearchOutputFormat(config, transports, new ElasticsearchSinkFunction[String] {
      def createIndexRequest(element: String): IndexRequest = {
        Requests.indexRequest.index("iteblog").`type`("info").source(element)
      }

      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
        indexer.add(createIndexRequest(element))
      }
}))
```

## Using in Java
```java
Map<String, String> config = new HashMap<>();
config.put("bulk.flush.max.actions", "1000");
config.put("cluster.name", "elasticsearch");

String hosts = "www.iteblog.com";

List<InetSocketAddress> list = Lists.newArrayList();
for (String host : hosts.split(",")) {
    list.add(new InetSocketAddress(InetAddress.getByName(host), 9300));
}

DataSet<String> data  = ....;

data.output(new ElasticSearchOutputFormat<>(config, list, new ElasticsearchSinkFunction<String>() {
    @Override
    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
    }

    private IndexRequest createIndexRequest(String element) {
        return Requests.indexRequest().index("iteblog").type("info").source(element);
    }
}));
```