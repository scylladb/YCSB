/*
 * Copyright (c) 2015-2026 YCSB contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package site.ycsb.db.alternator;

import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointParams;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;

import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public final class AlternatorEndpointProvider implements DynamoDbEndpointProvider, Closeable {

  private final AlternatorLoadBalancer loadBalancer;
  private final Map<URI, CompletableFuture<Endpoint>> endpointCache;

  private AlternatorEndpointProvider(AlternatorLoadBalancer loadBalancer) {
    this.loadBalancer = loadBalancer;
    this.endpointCache = new ConcurrentHashMap<>();
  }


  public static Builder builder() {
    return new Builder();
  }

  @Override
  public CompletableFuture<Endpoint> resolveEndpoint(DynamoDbEndpointParams params) {
    var node = loadBalancer.nextNode();
    return endpointCache.computeIfAbsent(node, uri ->
        CompletableFuture.completedFuture(Endpoint.builder().url(uri).build()));
  }

  public void markNodeFailed(URI node) {
    loadBalancer.markNodeFailed(node);
  }

  public void markNodeSuccess(URI node) {
    loadBalancer.markNodeSuccess(node);
  }

  public int getNodeCount() {
    return loadBalancer.getNodeCount();
  }

  public int getHealthyNodeCount() {
    return loadBalancer.getHealthyNodeCount();
  }

  public Duration timeSinceLastRefresh() {
    return loadBalancer.timeSinceLastRefresh();
  }

  @Override
  public void close() {
    loadBalancer.close();
    endpointCache.clear();
  }

  public static final class Builder {
    private URI seedUri;
    private String datacenter = "";
    private String rack = "";
    private boolean trustAllCertificates;
    private Duration refreshInterval;
    private Duration httpTimeout;

    private Builder() {}

    public Builder seedUri(URI uri) {
      this.seedUri = Objects.requireNonNull(uri, "seedUri");
      return this;
    }

    public Builder datacenter(String dc) {
      this.datacenter = Optional.ofNullable(dc).orElse("");
      return this;
    }

    public Builder rack(String r) {
      this.rack = Optional.ofNullable(r).orElse("");
      return this;
    }

    public Builder trustAllCertificates(boolean trust) {
      this.trustAllCertificates = trust;
      return this;
    }

    public Builder refreshInterval(Duration interval) {
      this.refreshInterval = interval;
      return this;
    }

    public Builder httpTimeout(Duration timeout) {
      this.httpTimeout = timeout;
      return this;
    }

    public AlternatorEndpointProvider build() {
      Objects.requireNonNull(seedUri, "seedUri is required");

      var lbBuilder = AlternatorLoadBalancer.builder()
          .seedUri(seedUri)
          .datacenter(datacenter)
          .rack(rack)
          .trustAllCertificates(trustAllCertificates);

      if (refreshInterval != null) {
        lbBuilder.refreshInterval(refreshInterval);
      }
      if (httpTimeout != null) {
        lbBuilder.httpTimeout(httpTimeout);
      }

      var lb = lbBuilder.build();
      lb.start();
      return new AlternatorEndpointProvider(lb);
    }
  }
}
