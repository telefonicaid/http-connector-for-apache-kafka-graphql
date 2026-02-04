/*
 * Copyright 2023 Aiven Oy and http-connector-for-apache-kafka project contributors
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
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.sender.DefaultHttpSender.DefaultHttpRequestBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BasicAuthHttpSender extends AbstractHttpSender implements HttpSender {

    BasicAuthHttpSender(
        final HttpSinkConfig config,
        final HttpClient client,
        final BasicAuthAccessTokenHttpSender basicAuthAccessTokenHttpSender
    ) {
        super(config, new BasicAuthHttpRequestBuilder(config, basicAuthAccessTokenHttpSender), client);
    }

    @Override
    protected HttpResponse<String> sendWithRetries(
        final Builder requestBuilder, final HttpResponseHandler originHttpResponseHandler, final int retries
    ) {
        // This handler allows to request a new access token if a 401 occurs, meaning the session might be expired
        final HttpResponseHandler handler = (response, remainingRetries) -> {
            // If the response has a 401 error and we have retries left, we attempt to renew the session
            if (response.statusCode() == 401 && remainingRetries > 0) {
                // Update the request builder with the new access token
                ((BasicAuthHttpRequestBuilder) this.httpRequestBuilder).renewAccessToken(requestBuilder);
                // Retry the call and decrease the retries counter to avoid looping on token renewal
                this.sendWithRetries(requestBuilder, originHttpResponseHandler, remainingRetries - 1);
            } else {
                originHttpResponseHandler.onResponse(response, remainingRetries);
            }
        };
        return super.sendWithRetries(requestBuilder, handler, retries);
    }

    private static class BasicAuthHttpRequestBuilder extends DefaultHttpRequestBuilder {

        private static final Logger LOGGER = LoggerFactory.getLogger(BasicAuthHttpRequestBuilder.class);
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        private final HttpSinkConfig config;
        private final BasicAuthAccessTokenHttpSender basicAuthAccessTokenHttpSender;

        private String accessToken;

        BasicAuthHttpRequestBuilder(
            final HttpSinkConfig config,
            final BasicAuthAccessTokenHttpSender basicAuthAccessTokenHttpSender
        ) {
            this.config = config;
            this.basicAuthAccessTokenHttpSender = basicAuthAccessTokenHttpSender;
        }

        
        @Override
        public Builder build(final HttpSinkConfig config) {
            return super
                .build(config)
                .header(HEADER_AUTHORIZATION, requestAccessToken());
        }


        /**
         * When expired, reinitialize the current token, request a new one and update the request builder
         *
         * @param requestBuilder the request builder used to call the protected URI
         */
        void renewAccessToken(final HttpRequest.Builder requestBuilder) {
            this.accessToken = null;
            requestBuilder.setHeader(HttpRequestBuilder.HEADER_AUTHORIZATION, this.requestAccessToken());
        }
        
        /**
         * Retrieves the current access token or requests it if none is defined
         *
         * @return an access token
         */
        private String requestAccessToken() {
            // Re-use the access token if it's already defined
            if (this.accessToken != null) {
                return this.accessToken;
            }
            LOGGER.info("Configure BasicAuth for URI: {} and Client ID: {}", config.basicAuthAccessTokenUri(),
                        config.basicAuthClientId());
            try {
                // Whenever the access token is null (not initialized yet or expired), call the AccessTokenHttpSender
                // implementation to request one
                final var response = basicAuthAccessTokenHttpSender.call();
                accessToken = buildAccessTokenAuthHeader(response.body());
            } catch (final IOException e) {
                throw new ConnectException("Couldn't get BasicAuth access token", e);
            }
            return accessToken;
        }

        private String buildAccessTokenAuthHeader(final String basicAuthResponseBody) throws JsonProcessingException {
            final var accessTokenResponse =
                OBJECT_MAPPER.readValue(basicAuthResponseBody, new TypeReference<Map<String, String>>() {});
            if (!accessTokenResponse.containsKey(config.basicAuthResponseTokenProperty())) {
                throw new ConnectException("Couldn't find access token property "
                                           + config.basicAuthResponseTokenProperty()
                                           + " in response properties: " + accessTokenResponse.keySet());
            }
            final var tokenType = accessTokenResponse.getOrDefault("token_type", "Bearer");
            final var accessToken = accessTokenResponse.get(config.basicAuthResponseTokenProperty());
            return String.format("%s %s", tokenType, accessToken);
        }

        
        
    }

}
