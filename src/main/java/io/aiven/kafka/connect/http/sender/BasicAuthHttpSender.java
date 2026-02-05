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

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicAuthHttpSender.class);

    BasicAuthHttpSender(
        final HttpSinkConfig config,
        final HttpClient client,
        final BasicAuthAccessTokenHttpSender basicAuthAccessTokenHttpSender
    ) {
        super(config, new BasicAuthHttpRequestBuilder(config, basicAuthAccessTokenHttpSender), client);
    }

    @Override
    protected HttpResponse<String> sendWithRetries(
        final Builder requestBuilder,
        final HttpResponseHandler originHandler,
        final int retries
    ) {
        final HttpResponseHandler composedHandler = (response, remainingRetries) -> {
            final int status = response.statusCode();
            LOGGER.info("response status{}", status);

            // If we got Unauthorized (or Forbidden) and we still have retries left,
            // renew the access token and force AbstractHttpSender to retry by throwing IOException.
            if ((status == 401 || status == 403) && remainingRetries > 0) {
                ((BasicAuthHttpRequestBuilder) this.httpRequestBuilder).renewAccessToken(requestBuilder);
                throw new IOException(status + " received: renewed access token, retrying");
            }
            // Keep existing logic (GraphQL 200 with errors[], >=400, etc.)
            originHandler.onResponse(response, remainingRetries);
        };
        return super.sendWithRetries(requestBuilder, composedHandler, retries);
    }

    private static class BasicAuthHttpRequestBuilder extends DefaultHttpRequestBuilder {

        private static final Logger LOGGER = LoggerFactory.getLogger(BasicAuthHttpRequestBuilder.class);
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        private final HttpSinkConfig config;
        private final BasicAuthAccessTokenHttpSender basicAuthAccessTokenHttpSender;
        private static final String ACCESS_TOKEN_FIELD = "access_token";
        private volatile String accessToken;
        private final Object tokenLock = new Object();

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
            synchronized (tokenLock) {
                this.accessToken = null;
            }
            LOGGER.info("renewAccessToken using BasicAuth for URI: {} and Client ID: {}",
                        config.basicAuthAccessTokenUri(),
                        config.basicAuthClientId());
            requestBuilder.setHeader(HttpRequestBuilder.HEADER_AUTHORIZATION, this.requestAccessToken());
        }

        /**
         * Retrieves the current access token or requests it if none is defined
         *
         * @return an access token
         */
        private String requestAccessToken() {
            LOGGER.info("requestAccessToken using BasicAuth for URI: {} and Client ID: {}",
                        config.basicAuthAccessTokenUri(),
                        config.basicAuthClientId());
            String token = this.accessToken;
            if (token != null) {
                return token;
            }

            synchronized (tokenLock) {
                token = this.accessToken;
                if (token != null) {
                    return token;
                }
                LOGGER.info("Requesting BasicAuth token from URI: {} for clientId: {}",
                             config.basicAuthAccessTokenUri(), config.basicAuthClientId());
                try {
                    // Whenever the access token is null (not initialized yet or expired),
                    // call the AccessTokenHttpSender implementation to request one
                    final var response = basicAuthAccessTokenHttpSender.call();
                    token = buildAccessTokenAuthHeader(response.body());
                    this.accessToken = token;
                    return token;
                } catch (final IOException e) {
                    throw new ConnectException("Couldn't get BasicAuth access token", e);
                }
            }
        }

        private String buildAccessTokenAuthHeader(final String basicAuthResponseBody) throws JsonProcessingException {
            final var accessTokenResponse =
                OBJECT_MAPPER.readValue(basicAuthResponseBody, new TypeReference<Map<String, String>>() {});
            if (!accessTokenResponse.containsKey(ACCESS_TOKEN_FIELD)) {
                throw new ConnectException("Couldn't find access token property "
                                           + ACCESS_TOKEN_FIELD
                                           + " in response properties: " + accessTokenResponse.keySet());
            }
            final var tokenType = accessTokenResponse.getOrDefault("token_type", "Bearer");
            final var accessToken = accessTokenResponse.get(ACCESS_TOKEN_FIELD);
            return String.format("%s %s", tokenType, accessToken);
        }

        
        
    }

}
