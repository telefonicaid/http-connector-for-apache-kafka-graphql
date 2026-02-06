/*
 * Copyright 2021 Aiven Oy and http-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.http.sender;

import java.io.IOException;
import java.net.http.HttpResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface HttpResponseHandler {

    Logger LOGGER = LoggerFactory.getLogger(HttpResponseHandler.class);
    final ObjectMapper MAPPER = new ObjectMapper();

    void onResponse(final HttpResponse<String> response, int remainingRetries) throws IOException;

    HttpResponseHandler ON_HTTP_ERROR_RESPONSE_HANDLER = (response, remainingRetries) -> {
        if (response.statusCode() == 200) {
            // GraphQL logic: response 200 with errors[] are like 400
            boolean isError = false;
            final Object value = response.body();
            if (value != null) {
                try {
                    JsonNode root = null;
                    if (value instanceof String) {
                        root = MAPPER.readTree((String) value);
                    } else {
                        // fallback: serialize value.toString()
                        root = MAPPER.readTree(value.toString());
                    }
                    if (root.has("errors")
                        && root.get("errors").isArray()
                        && root.get("errors").size() > 0) {
                        isError = true;
                    }
                } catch (IOException e) {
                    // ignore parse errors, treat as non-error
                }
            }
            if (isError) {
                final var request = response.request();
                final var uri = request != null ? request.uri() : "UNKNOWN";
                LOGGER.warn(
                            "Got 200 HTTP status code: {} with errors in body: {}. Requested URI: {}",
                            response.statusCode(),
                            response.body(),
                            uri);
                throw new IOException("Server replied with status code " + response.statusCode()
                                      + " and body with errors " + response.body());
            }
        } else if (response.statusCode() >= 400) {
            final var request = response.request();
            final var uri = request != null ? request.uri() : "UNKNOWN";
            LOGGER.warn(
                    "Got unexpected HTTP status code: {} and body: {}. Requested URI: {}",
                    response.statusCode(),
                    response.body(),
                    uri);
            throw new IOException("Server replied with status code " + response.statusCode()
                    + " and body " + response.body());
        }
    };

}
