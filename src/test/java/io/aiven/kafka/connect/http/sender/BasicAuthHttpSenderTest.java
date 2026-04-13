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
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BasicAuthHttpSenderTest extends HttpSenderTestBase {

    static final String ACCESS_TOKEN_RESPONSE =
        "{\"access_token\": \"my_access_token\",\"token_type\": \"Bearer\",\"expires_in\": 7199}";

    @Mock
    private BasicAuthAccessTokenHttpSender basicAuthAccessTokenHttpSender;

    @Test
    void shouldThrowExceptionWithoutConfig() {
        assertThrows(NullPointerException.class, () -> new BasicAuthHttpSender(null, null, null));
    }

    @Test
    void shouldBuildDefaultHttpRequest() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(basicAuthAccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse);

        final var httpSender = Mockito.spy(new BasicAuthHttpSender(config, mockedClient, basicAuthAccessTokenHttpSender));

        final List<String> messages = List.of("some message");
        messages.forEach(httpSender::send);

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
                assertThat(httpRequest.uri()).isEqualTo(config.httpUri());
                assertThat(httpRequest.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(httpRequest.method()).isEqualTo("POST");

                assertThat(httpRequest
                    .headers()
                    .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                    .orElse(null)).isEqualTo("Bearer my_access_token");
            });

        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)))
        );
        verify(mockedClient, times(messages.size())).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void reuseAccessToken() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(basicAuthAccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse);

        final var httpSender = Mockito.spy(new BasicAuthHttpSender(config, mockedClient, basicAuthAccessTokenHttpSender));

        final List<String> messages = List.of("some message 1", "some message 2");
        messages.forEach(httpSender::send);

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
                assertThat(httpRequest.uri()).isEqualTo(config.httpUri());
                assertThat(httpRequest.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(httpRequest.method()).isEqualTo("POST");

                assertThat(httpRequest
                    .headers()
                    .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                    .orElse(null)).isEqualTo("Bearer my_access_token");
            });

        verify(basicAuthAccessTokenHttpSender).call();

        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)))
        );
        verify(mockedClient, times(messages.size())).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void refreshAccessTokenOnUnauthorizedResponse() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        
        final HttpResponse<String> mockedAccessTokenResponseRefreshed = mock(HttpResponse.class);
        when(mockedAccessTokenResponseRefreshed.body()).thenReturn(
                                                                   "{\"access_token\": \"my_refreshed_token\",\"token_type\": \"Bearer\",\"expires_in\": 7199}");
        when(basicAuthAccessTokenHttpSender.call()).thenReturn(
                                                               mockedAccessTokenResponse, mockedAccessTokenResponseRefreshed);
        
        final HttpResponse<String> errorResponse = mock(HttpResponse.class);
        when(errorResponse.statusCode()).thenReturn(401);
        
        final HttpResponse<String> normalResponse = mock(HttpResponse.class);
        when(normalResponse.statusCode()).thenReturn(200);
        when(normalResponse.body()).thenReturn("{\"data\":{\"ok\":true}}");
        
        final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());
        
        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(
                                                                                           mockedResponse, errorResponse, normalResponse);
        
        final var httpSender = Mockito.spy(new BasicAuthHttpSender(config, mockedClient, basicAuthAccessTokenHttpSender));
        
        final List<String> messages = List.of("some message 1", "some message 2");
        messages.forEach(httpSender::send);
        
        final ArgumentCaptor<HttpRequest> httpRequestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockedClient, times(messages.size() + 1)).send(httpRequestCaptor.capture(), any(BodyHandler.class));
        
        final List<HttpRequest> httpRequests = httpRequestCaptor.getAllValues();
        
        assertThat(httpRequests).hasSize(3);
        
        httpRequests.forEach(httpRequest -> {
                assertThat(httpRequest.uri()).isEqualTo(config.httpUri());
                assertThat(httpRequest.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(httpRequest.method()).isEqualTo("POST");
            });
        
        assertThat(httpRequests.get(0)
                   .headers()
                   .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                   .orElse(null)).isEqualTo("Bearer my_access_token");

        assertThat(httpRequests.get(1)
                   .headers()
                   .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                   .orElse(null)).isEqualTo("Bearer my_access_token");
        
        assertThat(httpRequests.get(2)
                   .headers()
                   .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                   .orElse(null)).isEqualTo("Bearer my_refreshed_token");

        verify(basicAuthAccessTokenHttpSender, times(2)).call();
        
        messages.forEach(
                         message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)))
                         );
        verify(mockedClient, times(messages.size() + 1)).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void refreshAccessTokenOnGraphQlExpiredTokenResponse() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        
        final HttpResponse<String> mockedAccessTokenResponseRefreshed = mock(HttpResponse.class);
        when(mockedAccessTokenResponseRefreshed.body()).thenReturn(
                                                                   "{\"access_token\": \"my_refreshed_token\",\"token_type\": \"Bearer\",\"expires_in\": 7199}");
        when(basicAuthAccessTokenHttpSender.call()).thenReturn(
                                                               mockedAccessTokenResponse, mockedAccessTokenResponseRefreshed);
        
        final HttpResponse<String> expiredTokenGraphQlResponse = mock(HttpResponse.class);
        when(expiredTokenGraphQlResponse.statusCode()).thenReturn(200);
        when(expiredTokenGraphQlResponse.body()).thenReturn(
                                                            "{\"data\":{\"updateLocation\":null},"
                                                            + "\"errors\":[{\"message\":\"401: Token expired\",\"locations\":[{\"line\":3,\"column\":17}],"
                                                            + "\"path\":[\"updateLocation\"]}]}"
                                                            );
        
        final HttpResponse<String> normalResponse = mock(HttpResponse.class);
        when(normalResponse.statusCode()).thenReturn(200);
        when(normalResponse.body()).thenReturn("{\"data\":{\"updateLocation\":{\"id\":\"ok\"}}}");
        
        final HttpSinkConfig config = new HttpSinkConfig(defaultGraphQlConfig());
        
        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(
                                                                                           expiredTokenGraphQlResponse, normalResponse);
        
        final var httpSender = Mockito.spy(new BasicAuthHttpSender(config, mockedClient, basicAuthAccessTokenHttpSender));
        
        httpSender.send("some message");
        
        final ArgumentCaptor<HttpRequest> httpRequestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockedClient, times(2)).send(httpRequestCaptor.capture(), any(BodyHandler.class));
        
        final List<HttpRequest> httpRequests = httpRequestCaptor.getAllValues();

        assertThat(httpRequests).hasSize(2);
        
        httpRequests.forEach(httpRequest -> {
                assertThat(httpRequest.uri()).isEqualTo(config.httpUri());
                assertThat(httpRequest.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(httpRequest.method()).isEqualTo("POST");
            });
        
        assertThat(httpRequests.get(0)
                   .headers()
                   .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                   .orElse(null)).isEqualTo("Bearer my_access_token");
        
        assertThat(httpRequests.get(1)
                   .headers()
                   .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                   .orElse(null)).isEqualTo("Bearer my_refreshed_token");
        
        verify(basicAuthAccessTokenHttpSender, times(2)).call();
        verify(mockedClient, times(2)).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void throwsConnectExceptionForUnauthorizedToken() {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(basicAuthAccessTokenHttpSender.call()).thenReturn(
            mockedAccessTokenResponse, mockedAccessTokenResponse);

        final HttpResponse<String> errorResponse = mock(HttpResponse.class);
        when(errorResponse.statusCode()).thenReturn(401);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> {
                final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

                when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(errorResponse);

                final var httpSender =
                    Mockito.spy(new BasicAuthHttpSender(config, mockedClient, basicAuthAccessTokenHttpSender));

                final List<String> messages = List.of("some message 1", "some message 2");
                messages.forEach(httpSender::send);
            })
            .withMessageContaining("status code 401");

        verify(basicAuthAccessTokenHttpSender, times(2)).call();
    }

    @Test
    void throwsConnectExceptionForWrongAuthentication() throws IOException, InterruptedException {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn("not a json");
        when(basicAuthAccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> new BasicAuthHttpSender(
                new HttpSinkConfig(defaultConfig()),
                mockedClient,
                basicAuthAccessTokenHttpSender
            ).send("a message"))
            .withMessage("Couldn't get BasicAuth access token");

        verify(basicAuthAccessTokenHttpSender).call();
        verify(mockedClient, never()).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void throwsConnectExceptionForBadFormedAccessToken() throws IOException, InterruptedException {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(
            "{\"bad_property\": \"my_access_token\",\"token_type\": \"Bearer\",\"expires_in\": 7199}");
        when(basicAuthAccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> new BasicAuthHttpSender(
                new HttpSinkConfig(defaultConfig()),
                mockedClient,
                basicAuthAccessTokenHttpSender
            ).send("a message"))
            .withMessage("Couldn't find access token property access_token in"
                + " response properties: [bad_property, token_type, expires_in]");

        verify(basicAuthAccessTokenHttpSender).call();
        verify(mockedClient, never()).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void throwsConnectExceptionForServerError() {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(basicAuthAccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        final HttpResponse<String> errorResponse = mock(HttpResponse.class);
        when(errorResponse.statusCode()).thenReturn(500);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> {
                final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

                when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(errorResponse);

                final var httpSender =
                    Mockito.spy(new BasicAuthHttpSender(config, mockedClient, basicAuthAccessTokenHttpSender));

                final List<String> messages = List.of("some message 1", "some message 2");
                messages.forEach(httpSender::send);
            })
            .withMessageContaining("status code 500");
    }

    @Test
    void doesNotRefreshAccessTokenOnGraphQlInternalServerError() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(basicAuthAccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        final HttpResponse<String> graphQlErrorResponse = mock(HttpResponse.class);
        when(graphQlErrorResponse.statusCode()).thenReturn(200);
        when(graphQlErrorResponse.body()).thenReturn(
                                                     "{\"data\":{\"updateAdvice\":null},"
                                                     + "\"errors\":[{\"message\":\"Redis Cluster cannot be connected. "
                                                     + "Please provide at least one reachable node: None\","
                                                     + "\"locations\":[{\"line\":3,\"column\":17}],"
                                                     + "\"path\":[\"updateAdvice\"]}]}"
                                                     );

        final HttpSinkConfig config = new HttpSinkConfig(defaultGraphQlConfig());

        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(graphQlErrorResponse);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> {
                    final var httpSender =
                        Mockito.spy(new BasicAuthHttpSender(config, mockedClient, basicAuthAccessTokenHttpSender));
                    httpSender.send("some message");
                })
            .withMessageContaining("status code 200")
            .withMessageContaining("body with errors");

        // Token requested only once: initial token retrieval, no refresh
        verify(basicAuthAccessTokenHttpSender, times(1)).call();

        // With default max.retries=1, one initial attempt + one retry
        verify(mockedClient, times(2)).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void doesNotRefreshAccessTokenOnGraphQl401LikeErrorThatIsNotTokenExpired() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(basicAuthAccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        final HttpResponse<String> graphQlErrorResponse = mock(HttpResponse.class);
        when(graphQlErrorResponse.statusCode()).thenReturn(200);
        when(graphQlErrorResponse.body()).thenReturn(
                                                     "{\"data\":{\"updateAdvice\":null},"
                                                     + "\"errors\":[{\"message\":\"401: unauthorized operation for this resource\","
                                                     + "\"locations\":[{\"line\":3,\"column\":17}],"
                                                     + "\"path\":[\"updateAdvice\"]}]}"
                                                     );

        final HttpSinkConfig config = new HttpSinkConfig(defaultGraphQlConfig());

        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(graphQlErrorResponse);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> {
                    final var httpSender =
                        Mockito.spy(new BasicAuthHttpSender(config, mockedClient, basicAuthAccessTokenHttpSender));
                    httpSender.send("some message");
                })
            .withMessageContaining("status code 200")
            .withMessageContaining("body with errors");

        // Token requested only once: initial token retrieval, no refresh
        verify(basicAuthAccessTokenHttpSender, times(1)).call();

        // With default max.retries=1, one initial attempt + one retry
        verify(mockedClient, times(2)).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void refreshAccessTokenOnGraphQlExpiredTokenResponseCaseInsensitive() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);

        final HttpResponse<String> mockedAccessTokenResponseRefreshed = mock(HttpResponse.class);
        when(mockedAccessTokenResponseRefreshed.body()).thenReturn(
                                                                   "{\"access_token\": \"my_refreshed_token\",\"token_type\": \"Bearer\",\"expires_in\": 7199}");

        when(basicAuthAccessTokenHttpSender.call()).thenReturn(
                                                               mockedAccessTokenResponse, mockedAccessTokenResponseRefreshed);

        final HttpResponse<String> expiredTokenGraphQlResponse = mock(HttpResponse.class);
        when(expiredTokenGraphQlResponse.statusCode()).thenReturn(200);
        when(expiredTokenGraphQlResponse.body()).thenReturn(
                                                            "{\"data\":{\"updateLocation\":null},"
                                                            + "\"errors\":[{\"message\":\"401: TOKEN EXPIRED\","
                                                            + "\"locations\":[{\"line\":3,\"column\":17}],"
                                                            + "\"path\":[\"updateLocation\"]}]}"
                                                            );

        final HttpResponse<String> normalResponse = mock(HttpResponse.class);
        when(normalResponse.statusCode()).thenReturn(200);
        when(normalResponse.body()).thenReturn("{\"data\":{\"updateLocation\":{\"id\":\"ok\"}}}");

        final HttpSinkConfig config = new HttpSinkConfig(defaultGraphQlConfig());

        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(
                                                                                           expiredTokenGraphQlResponse, normalResponse);

        final var httpSender =
            Mockito.spy(new BasicAuthHttpSender(config, mockedClient, basicAuthAccessTokenHttpSender));

        httpSender.send("some message");

        final ArgumentCaptor<HttpRequest> httpRequestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockedClient, times(2)).send(httpRequestCaptor.capture(), any(BodyHandler.class));

        final List<HttpRequest> httpRequests = httpRequestCaptor.getAllValues();

        assertThat(httpRequests).hasSize(2);

        assertThat(httpRequests.get(0)
                   .headers()
                   .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                   .orElse(null)).isEqualTo("Bearer my_access_token");

        assertThat(httpRequests.get(1)
                   .headers()
                   .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                   .orElse(null)).isEqualTo("Bearer my_refreshed_token");

        verify(basicAuthAccessTokenHttpSender, times(2)).call();
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

    private Map<String, String> defaultGraphQlConfig() {
        return Map.of(
            "http.url", "http://localhost:42",
            "http.authorization.type", "basic",
            "basic.access.token.url", "http://localhost:42/token",
            "basic.client.id", "some_client_id",
            "basic.client.secret", "some_client_secret",
            "basic.username", "some_user",
            "basic.password", "some_password",
            "http.graphql.errors.as.http_error", "true"
        );
    }
}
