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

import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BasicAuthAccessTokenHttpSenderTest extends HttpSenderTestBase {

    @Test
    void shouldThrowExceptionWithoutConfig() {
        assertThrows(NullPointerException.class, () -> new BasicAuthAccessTokenHttpSender(null, null));
    }

    @Test
    void shouldBuildDefaultAccessTokenRequest() throws Exception {
        final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse);

        final var httpSender = Mockito.spy(new BasicAuthAccessTokenHttpSender(config, mockedClient));

        final List<String> messages = List.of(
            "grant_type=password"
                + "&client_id=some_client_id"
                + "&client_secret=some_client_secret"
                + "&username=some_user"
                + "&password=some_password"
        );
        httpSender.call();

        final ArgumentCaptor<Builder> defaultHttpRequestBuilder = ArgumentCaptor.forClass(HttpRequest.Builder.class);
        verify(httpSender, atLeast(messages.size())).sendWithRetries(
            defaultHttpRequestBuilder.capture(),
            any(HttpResponseHandler.class),
            anyInt()
        );

        defaultHttpRequestBuilder
            .getAllValues()
            .stream()
            .map(Builder::build)
            .forEach(httpRequest -> {
                assertThat(httpRequest.uri()).isEqualTo(config.basicAuthAccessTokenUri());
                assertThat(httpRequest.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(httpRequest.method()).isEqualTo("POST");

                assertThat(httpRequest.headers()
                    .firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE))
                    .hasValue("application/x-www-form-urlencoded");

                assertThat(httpRequest.headers()
                    .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION))
                    .isEmpty();
            });

        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)))
        );
    }

    @Test
    void shouldBuildAccessTokenRequestWithScope() throws Exception {
        final Map<String, String> configBase = new HashMap<>(defaultConfig());
        configBase.put("basic.client.scope", "scope1,scope2");

        final HttpSinkConfig config = new HttpSinkConfig(configBase);

        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse);

        final var httpSender = Mockito.spy(new BasicAuthAccessTokenHttpSender(config, mockedClient));

        final List<String> messages = List.of(
            "grant_type=password"
                + "&scope=scope1%2Cscope2"
                + "&client_id=some_client_id"
                + "&client_secret=some_client_secret"
                + "&username=some_user"
                + "&password=some_password"
        );
        httpSender.call();

        final ArgumentCaptor<Builder> defaultHttpRequestBuilder = ArgumentCaptor.forClass(HttpRequest.Builder.class);
        verify(httpSender, atLeast(messages.size())).sendWithRetries(
            defaultHttpRequestBuilder.capture(),
            any(HttpResponseHandler.class),
            anyInt()
        );

        defaultHttpRequestBuilder
            .getAllValues()
            .stream()
            .map(Builder::build)
            .forEach(httpRequest -> {
                assertThat(httpRequest.uri()).isEqualTo(config.basicAuthAccessTokenUri());
                assertThat(httpRequest.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(httpRequest.method()).isEqualTo("POST");

                assertThat(httpRequest.headers()
                    .firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE))
                    .hasValue("application/x-www-form-urlencoded");

                assertThat(httpRequest.headers()
                    .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION))
                    .isEmpty();
            });

        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)))
        );
    }

    @Test
    void throwsConnectExceptionForServerError() {
        final HttpResponse<String> errorResponse = mock(HttpResponse.class);
        when(errorResponse.statusCode()).thenReturn(500);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> {
                final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

                when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(errorResponse);

                final var httpSender = Mockito.spy(new BasicAuthAccessTokenHttpSender(config, mockedClient));

                final List<String> messages = List.of("some message 1", "some message 2");
                messages.forEach(httpSender::send);
            })
            .withMessageContaining("status code 500");
    }

    private Map<String, String> defaultConfig() {
        return Map.of(
            "http.url", "http://localhost:42",
            "http.authorization.type", "basic",
            "basic.access.token.url", "http://localhost:42/token",
            "basic.client.id", "some_client_id",
            "basic.client.secret", "some_client_secret",
            "basic.username", "some_user",
            "basic.password", "some_password"
        );
    }
}
