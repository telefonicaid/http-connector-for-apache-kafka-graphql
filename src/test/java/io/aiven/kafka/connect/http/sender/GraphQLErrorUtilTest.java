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
