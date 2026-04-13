package io.aiven.kafka.connect.http.sender;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GraphQlErrorUtilsTest {

    @Test
    void detectsExpiredToken() {
        final String body =
            "{"
                + "\"errors\": ["
                + "{ \"message\": \"401: Token expired\" }"
                + "]"
                + "}";

        assertThat(GraphQlErrorUtils.isExpiredToken(body)).isTrue();
    }

    @Test
    void detectsExpiredTokenCaseInsensitive() {
        final String body =
            "{"
                + "\"errors\": ["
                + "{ \"message\": \"401: TOKEN EXPIRED\" }"
                + "]"
                + "}";

        assertThat(GraphQlErrorUtils.isExpiredToken(body)).isTrue();
    }

    @Test
    void detectsJwtExpired() {
        final String body =
            "{"
                + "\"errors\": ["
                + "{ \"message\": \"401: jwt expired\" }"
                + "]"
                + "}";

        assertThat(GraphQlErrorUtils.isExpiredToken(body)).isTrue();
    }

    @Test
    void detectsAccessTokenExpired() {
        final String body =
            "{"
                + "\"errors\": ["
                + "{ \"message\": \"401: access token expired\" }"
                + "]"
                + "}";

        assertThat(GraphQlErrorUtils.isExpiredToken(body)).isTrue();
    }

    @Test
    void doesNotDetectOtherErrors() {
        final String body =
            "{"
                + "\"errors\": ["
                + "{ \"message\": \"Redis cluster not reachable\" }"
                + "]"
                + "}";

        assertThat(GraphQlErrorUtils.isExpiredToken(body)).isFalse();
    }

    @Test
    void doesNotDetectNonExpired401() {
        final String body =
            "{"
                + "\"errors\": ["
                + "{ \"message\": \"401: unauthorized operation\" }"
                + "]"
                + "}";

        assertThat(GraphQlErrorUtils.isExpiredToken(body)).isFalse();
    }

    @Test
    void doesNotDetectWhenNoErrorsField() {
        final String body =
            "{"
                + "\"data\": {"
                + "\"something\": \"ok\""
                + "}"
                + "}";

        assertThat(GraphQlErrorUtils.isExpiredToken(body)).isFalse();
    }

    @Test
    void doesNotDetectWhenErrorsIsEmpty() {
        final String body =
            "{"
                + "\"errors\": []"
                + "}";

        assertThat(GraphQlErrorUtils.isExpiredToken(body)).isFalse();
    }

    @Test
    void returnsFalseForInvalidJson() {
        assertThat(GraphQlErrorUtils.isExpiredToken("not-json")).isFalse();
    }

    @Test
    void returnsFalseForNullBody() {
        assertThat(GraphQlErrorUtils.isExpiredToken(null)).isFalse();
    }

    @Test
    void returnsFalseForEmptyBody() {
        assertThat(GraphQlErrorUtils.isExpiredToken("")).isFalse();
    }
}
