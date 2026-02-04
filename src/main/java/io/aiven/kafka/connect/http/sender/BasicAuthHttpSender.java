package io.aiven.kafka.connect.http.sender;

import java.net.http.HttpClient;
import java.net.http.HttpRequest.Builder;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.sender.DefaultHttpSender.DefaultHttpRequestBuilder;

class BasicAuthHttpSender extends AbstractHttpSender implements HttpSender {

    BasicAuthHttpSender(
        final HttpSinkConfig config,
        final HttpClient client,
        final BasicAuthAccessTokenHttpSender basicAuthAccessTokenHttpSender
    ) {
        super(config, new BasicAuthHttpRequestBuilder(), basicAuthAccessTokenHttpSender, client);
    }

    private static class BasicAuthHttpRequestBuilder extends DefaultHttpRequestBuilder {

        BasicAuthHttpRequestBuilder(
            final HttpSinkConfig config,
            final BasicAuthAccessTokenHttpSender basicAuthccessTokenHttpSender
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
            LOGGER.info("Configure BasicAuth for URI: {} and Client ID: {}", config.basicAuthTokenUri()(),
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
                throw new ConnectException("Couldn't find access token property " + config.basicAuthResponseTokenProperty()
                                           + " in response properties: " + accessTokenResponse.keySet());
            }
            final var tokenType = accessTokenResponse.getOrDefault("token_type", "Bearer");
            final var accessToken = accessTokenResponse.get(config.basicAuthResponseTokenProperty());
            return String.format("%s %s", tokenType, accessToken);
        }

        
        
    }

}
