package io.aiven.kafka.connect.http.sender;

import java.io.IOException;
import java.util.Locale;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class to inspect GraphQL responses and detect specific error conditions.
 */
final class GraphQlErrorUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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
}
