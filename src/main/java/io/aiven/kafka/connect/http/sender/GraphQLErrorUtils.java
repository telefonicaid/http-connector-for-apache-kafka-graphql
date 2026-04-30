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
import java.util.Arrays;
import java.util.Locale;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class to inspect GraphQL responses and detect specific error conditions.
 */
final class GraphQlErrorUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String[] NON_RETRYABLE_ERROR_PATTERNS = {
        "already exists"
    };

    enum ErrorDisposition {
        NONE,
        NON_RETRYABLE_ONLY,
        RETRYABLE
    }

    private GraphQlErrorUtils() {
        // Utility class
    }

    /**
     * Detects whether a GraphQL response body contains an expired access token error.
     *
     * Expected patterns (case-insensitive), typically inside errors[].message:
     * - "401: Token expired"
     * - "jwt expired"
     * - "access token expired"
     *
     * @param body GraphQL response body
     * @return true if an expired token is detected, false otherwise
     */
    static boolean isExpiredToken(final String body) {
        if (body == null || body.isBlank()) {
            return false;
        }

        try {
            final JsonNode root = MAPPER.readTree(body);
            final JsonNode errors = root.get("errors");

            if (errors == null || !errors.isArray()) {
                return false;
            }

            for (final JsonNode error : errors) {
                final String message = error.has("message")
                    ? error.get("message").asText("")
                    : "";

                final String normalized = message.toLowerCase(Locale.ROOT);

                if (normalized.contains("401")
                    && (normalized.contains("token expired")
                        || normalized.contains("expired token")
                        || normalized.contains("jwt expired")
                        || normalized.contains("access token expired"))) {
                    return true;
                }
            }

            return false;

        } catch (IOException e) {
            // If parsing fails, we assume it's not a GraphQL error response
            return false;
        }
    }

    static ErrorDisposition classifyErrors(final String body) {
        if (body == null || body.isBlank()) {
            return ErrorDisposition.NONE;
        }

        try {
            final JsonNode root = MAPPER.readTree(body);
            final JsonNode errors = root.get("errors");

            if (errors == null || !errors.isArray() || errors.isEmpty()) {
                return ErrorDisposition.NONE;
            }

            for (final JsonNode error : errors) {
                final String message = error.has("message")
                    ? error.get("message").asText("")
                    : error.toString();

                final String normalized = message.toLowerCase(Locale.ROOT);
                final boolean isNonRetryable = Arrays.stream(NON_RETRYABLE_ERROR_PATTERNS)
                    .anyMatch(normalized::contains);

                if (!isNonRetryable) {
                    return ErrorDisposition.RETRYABLE;
                }
            }

            return ErrorDisposition.NON_RETRYABLE_ONLY;
        } catch (IOException e) {
            // Keep previous behavior: parse failures should not trigger GraphQL error handling.
            return ErrorDisposition.NONE;
        }
    }
}
