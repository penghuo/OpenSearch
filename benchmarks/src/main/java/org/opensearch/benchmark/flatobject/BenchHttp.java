/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.flatobject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * A thin REST client for the flat_object benchmark driver.
 *
 * <p>Uses the JDK's own HTTP client so the harness adds no dependency of its own; the driver is a measurement tool and
 * anything it pulls in is something that could perturb what is being measured.
 */
final class BenchHttp {

    private final HttpClient client;
    private final String host;

    BenchHttp(String host) {
        this.host = host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).version(HttpClient.Version.HTTP_1_1).build();
    }

    String get(String path) throws IOException, InterruptedException {
        return send(request(path).GET().build());
    }

    String put(String path, String body) throws IOException, InterruptedException {
        return send(request(path).PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build());
    }

    String post(String path, String body) throws IOException, InterruptedException {
        return send(request(path).POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build());
    }

    String postBytes(String path, byte[] body) throws IOException, InterruptedException {
        return send(request(path).POST(HttpRequest.BodyPublishers.ofByteArray(body)).build());
    }

    String delete(String path) throws IOException, InterruptedException {
        return send(request(path).DELETE().build());
    }

    /**
     * Sends a request, tolerating a 404 so callers can delete an index that may not exist.
     */
    String deleteIfExists(String path) throws IOException, InterruptedException {
        HttpResponse<String> response = client.send(request(path).DELETE().build(), HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder()
            .uri(URI.create(host + path))
            .timeout(Duration.ofMinutes(10))
            .header("Content-Type", "application/json");
    }

    private String send(HttpRequest request) throws IOException, InterruptedException {
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IOException(
                "request " + request.method() + " " + request.uri() + " failed with " + response.statusCode() + ": " + response.body()
            );
        }
        return response.body();
    }

    /**
     * Extracts a numeric JSON field by key. Deliberately crude: the driver only needs a handful of scalars out of
     * responses whose shape is fixed, and a real JSON parse would mean pulling a parser into the measurement path.
     */
    static long extractLong(String json, String key) {
        String needle = "\"" + key + "\":";
        int at = json.indexOf(needle);
        if (at < 0) {
            return -1;
        }
        int start = at + needle.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        if (end == start) {
            return -1;
        }
        return Long.parseLong(json.substring(start, end));
    }

    static String extractString(String json, String key) {
        String needle = "\"" + key + "\":\"";
        int at = json.indexOf(needle);
        if (at < 0) {
            return null;
        }
        int start = at + needle.length();
        int end = json.indexOf('"', start);
        return end < 0 ? null : json.substring(start, end);
    }
}
