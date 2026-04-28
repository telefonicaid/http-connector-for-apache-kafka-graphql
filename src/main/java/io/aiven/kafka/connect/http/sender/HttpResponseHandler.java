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

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface HttpResponseHandler {

    Logger LOGGER = LoggerFactory.getLogger(HttpResponseHandler.class);

    void onResponse(final HttpResponse<String> response,
                    int remainingRetries,
                    HttpSinkConfig config) throws IOException;

    HttpResponseHandler ON_HTTP_ERROR_RESPONSE_HANDLER = (response, remainingRetries, config) -> {
        if (config.graphqlErrorsAsHttpError() && response.statusCode() == 200) {
            final GraphQlErrorUtils.ErrorDisposition errorDisposition =
                GraphQlErrorUtils.classifyErrors(response.body());
            if (errorDisposition == GraphQlErrorUtils.ErrorDisposition.RETRYABLE) {
                final var request = response.request();
                final var uri = request != null ? request.uri() : "UNKNOWN";
                LOGGER.warn(
                            "Got 200 HTTP status code with retryable GraphQL errors in body: {}. Requested URI: {}",
                            response.body(),
                            uri);
                throw new IOException("Server replied with status code " + response.statusCode()
                                      + " and body with retryable GraphQL errors " + response.body());
            }
            if (errorDisposition == GraphQlErrorUtils.ErrorDisposition.NON_RETRYABLE_ONLY) {
                final var request = response.request();
                final var uri = request != null ? request.uri() : "UNKNOWN";
                LOGGER.info(
                    "Ignoring non-retryable GraphQL errors in HTTP 200 response body: {}. Requested URI: {}",
                    response.body(),
                    uri);
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
