/*
 * Copyright 2021 Aiven Oy and http-connector-for-apache-kafka project contributors
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

package io.aiven.kafka.connect.http.sender.request;

import java.util.Objects;

final class PropertyValue {

    private final String property;
    private final String value;

    PropertyValue(final String property, final String value) {
        this.property = Objects.requireNonNull(property, "property must not be null");
        this.value = Objects.requireNonNull(value, "value must not be null");
    }

    String property() {
        return property;
    }

    String value() {
        return value;
    }
}
