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

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;

import io.aiven.kafka.connect.http.config.BasicAuthAuthorizationMode;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.sender.request.BasicAuthAccessTokenRequestForm;

import static io.aiven.kafka.connect.http.config.BasicAuthAuthorizationMode.HEADER;

class BasicAuthAccessTokenHttpSender extends AbstractHttpSender implements HttpSender {

    BasicAuthAccessTokenHttpSender(final HttpSinkConfig config, final HttpClient httpClient) {
        super(config, new AccessTokenHttpRequestBuilder(), httpClient);
    }

    HttpResponse<String> call() {
        final BasicAuthAccessTokenRequestForm.Builder formBuilder = BasicAuthAccessTokenRequestForm
            .newBuilder()
            .withGrantTypeProperty(config.oauth2GrantTypeProperty())
            .withGrantType(config.oauth2GrantType())
            .withScope(config.oauth2ClientScope());

        if (config.basicAuthAuthorizationMode() == BasicAuthAuthorizationMode.URL) {
            formBuilder
                .withClientIdProperty(config.basicAuthClientIdProperty())
                .withClientId(config.basicAuthClientId())
                .withClientSecretProperty(config.basicAuthClientSecretProperty())
                .withClientSecret(config
                    .basicAuthClientSecret()
                    .value())
                .withUsernameProperty(config.basicAuthUsernameProperty())
                .withUsername(config.basicAuthUsername())
                .withPasswordProperty(config.basicAuthPasswordProperty())
                .withPassword(config
                    .basicAuthPassword()
                    .value());
        }
        return super.send(formBuilder
            .build()
            .toBodyString());
    }

    private static class AccessTokenHttpRequestBuilder implements HttpRequestBuilder {

        static final String HEADER_CONTENT_TYPE_FORM = "application/x-www-form-urlencoded";

        protected AccessTokenHttpRequestBuilder() {
        }

        @Override
        public HttpRequest.Builder build(final HttpSinkConfig config) {
            final var builder = HttpRequest
                .newBuilder(Objects.requireNonNull(config.oauth2AccessTokenUri()))
                .timeout(Duration.ofSeconds(config.httpTimeout()))
                .header(HEADER_CONTENT_TYPE, HEADER_CONTENT_TYPE_FORM);
            if (config.basicAuthAuthorizationMode() == HEADER) {
                addClientIdAndSecretInRequestHeader(config, builder);
            }
            return builder;
        }

        private void addClientIdAndSecretInRequestHeader(
            final HttpSinkConfig config, final HttpRequest.Builder builder
        ) {
            final var clientAndSecretBytes = (config.oauth2ClientId() + ":" + config
                .oauth2ClientSecret()
                .value()).getBytes(StandardCharsets.UTF_8);
            final var clientAndSecretAuthHeader = "Basic " + Base64
                .getEncoder()
                .encodeToString(clientAndSecretBytes);
            builder.header(HEADER_AUTHORIZATION, clientAndSecretAuthHeader);
        }

    }

}
