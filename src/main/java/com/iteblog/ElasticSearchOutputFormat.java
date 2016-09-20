/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.iteblog;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by http://www.iteblog.com on 2016/9/9.
 */
public class ElasticSearchOutputFormat<T> extends RichOutputFormat<T> {
    public static final String CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
    public static final String CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB = "bulk.flush.max.size.mb";
    public static final String CONFIG_KEY_BULK_FLUSH_INTERVAL_MS = "bulk.flush.interval.ms";

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchOutputFormat.class);

    /**
     * The user specified config map that we forward to Elasticsearch when we create the Client.
     */
    private final Map<String, String> userConfig;

    /**
     * The list of nodes that the TransportClient should connect to. This is null if we are using
     * an embedded Node to get a Client.
     */
    private final List<InetSocketAddress> transportAddresses;

    /**
     * The builder that is used to construct an {@link IndexRequest} from the incoming element.
     */
    private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

    /**
     * The Client that was either retrieved from a Node or is a TransportClient.
     */
    private transient Client client;

    /**
     * Bulk processor that was created using the client
     */
    private transient BulkProcessor bulkProcessor;

    /**
     * Bulk {@link org.elasticsearch.action.ActionRequest} indexer
     */
    private transient RequestIndexer requestIndexer;

    /**
     * This is set from inside the BulkProcessor listener if there where failures in processing.
     */
    private final AtomicBoolean hasFailure = new AtomicBoolean(false);

    /**
     * This is set from inside the BulkProcessor listener if a Throwable was thrown during processing.
     */
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<Throwable>();

    public ElasticSearchOutputFormat(Map<String, String> userConfig, List<InetSocketAddress> transportAddresses, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
        this.userConfig = userConfig;
        this.transportAddresses = transportAddresses;
        Preconditions.checkArgument(transportAddresses != null && transportAddresses.size() > 0);
        this.elasticsearchSinkFunction = elasticsearchSinkFunction;
    }

    public void configure(Configuration configuration) {
        List<TransportAddress> transportNodes = new ArrayList<TransportAddress>(transportAddresses.size());
        for (InetSocketAddress address : transportAddresses) {
            transportNodes.add(new InetSocketTransportAddress(address));
        }

        Settings settings = Settings.settingsBuilder().put(userConfig).build();

        TransportClient transportClient = TransportClient.builder().settings(settings).build();
        for (TransportAddress transport : transportNodes) {
            transportClient.addTransportAddress(transport);
        }

        // verify that we actually are connected to a cluster
        ImmutableList<DiscoveryNode> nodes = ImmutableList.copyOf(transportClient.connectedNodes());
        if (nodes.isEmpty()) {
            throw new RuntimeException("Client is not connected to any Elasticsearch nodes!");
        }

        client = transportClient;

        if (LOG.isInfoEnabled()) {
            LOG.info("Created Elasticsearch TransportClient {}", client);
        }

        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    for (BulkItemResponse itemResp : response.getItems()) {
                        if (itemResp.isFailed()) {
                            LOG.error("Failed to index document in Elasticsearch: " + itemResp.getFailureMessage());
                            failureThrowable.compareAndSet(null, new RuntimeException(itemResp.getFailureMessage()));
                        }
                    }
                    hasFailure.set(true);
                }
            }

            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                LOG.error(failure.getMessage());
                failureThrowable.compareAndSet(null, failure);
                hasFailure.set(true);
            }
        });

        // This makes flush() blocking
        bulkProcessorBuilder.setConcurrentRequests(0);

        if (userConfig.containsKey(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
            bulkProcessorBuilder.setBulkActions(stringToInt(userConfig.get(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)));
        }

        if (userConfig.containsKey(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
            bulkProcessorBuilder.setBulkSize(new ByteSizeValue(stringToInt(
                    CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB), ByteSizeUnit.MB));
        }

        if (userConfig.containsKey(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)) {
            bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(stringToInt(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)));
        }

        bulkProcessor = bulkProcessorBuilder.build();
        requestIndexer = new BulkProcessorIndexer(bulkProcessor);
    }

    public void open(int taskNumber, int numTasks) throws IOException {

    }

    public void writeRecord(T element) throws IOException {
        elasticsearchSinkFunction.process(element, getRuntimeContext(), requestIndexer);
    }

    public void close() throws IOException {
        if (bulkProcessor != null) {
            bulkProcessor.close();
            bulkProcessor = null;
        }

        if (client != null) {
            client.close();
            client = null;
        }

        if (hasFailure.get()) {
            Throwable cause = failureThrowable.get();
            if (cause != null) {
                throw new RuntimeException("An error occured in ElasticSearchOutputFormat.", cause);
            } else {
                throw new RuntimeException("An error occured in ElasticSearchOutputFormat.");
            }
        }
    }

    private int stringToInt(String str) throws NumberFormatException {
        return Integer.valueOf(str);
    }
}
