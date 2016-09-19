# flink-elasticsearch2-connector

Flink DataSet ElasticSearchOutputFormat create by <https://www.iteblog.com>

# Usage

## pom.xml
```xml
<dependency>
       <groupId>com.iteblog</groupId>
       <artifactId>flink-elasticsearch2-connector</artifactId>
       <version>1.0.1</version>
</dependency>
```

## Using in scala
```scala
import scala.collection.JavaConversions._
val config = Map("bulk.flush.max.actions" -> "1000", "cluster.name" -> "p_pay_realtime_binlog_qes")
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