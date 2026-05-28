/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.protocol.v2.adapter;

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.protocol.v2.SelectedRoleType;
import com.facebook.presto.protocol.v2.StringMap;
import com.facebook.presto.spi.security.SelectedRole;

import java.util.Map;

import static com.facebook.presto.protocol.v2.adapter.ProtocolAdapters.setOptionalString;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class SessionRepresentationAdapter
        implements ProtocolAdapter<SessionRepresentation, com.facebook.presto.protocol.v2.SessionRepresentation>
{
    @Override
    public com.facebook.presto.protocol.v2.SessionRepresentation toProtocol(SessionRepresentation value)
    {
        requireNonNull(value, "value is null");

        com.facebook.presto.protocol.v2.SessionRepresentation.Builder builder = com.facebook.presto.protocol.v2.SessionRepresentation.newBuilder()
                .setQueryId(value.getQueryId())
                .setClientTransactionSupport(value.isClientTransactionSupport())
                .setUser(value.getUser())
                .setTimeZoneKey(value.getTimeZoneKey().getKey())
                .setLocale(value.getLocale().toLanguageTag())
                .addAllClientTags(value.getClientTags())
                .setStartTime(value.getStartTime())
                .putAllSystemProperties(value.getSystemProperties())
                .putAllCatalogProperties(value.getCatalogProperties().entrySet().stream()
                        .collect(toMap(entry -> entry.getKey().getCatalogName(), entry -> toStringMap(entry.getValue()))))
                .putAllUnprocessedCatalogProperties(value.getUnprocessedCatalogProperties().entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, entry -> toStringMap(entry.getValue()))))
                .putAllRoles(value.getRoles().entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, entry -> toProtocol(entry.getValue()))))
                .putAllPreparedStatements(value.getPreparedStatements());

        value.getTransactionId().ifPresent(transactionId -> builder.setTransactionId(transactionId.toString()));
        setOptionalString(value.getPrincipal(), builder::setPrincipal);
        setOptionalString(value.getSource(), builder::setSource);
        setOptionalString(value.getCatalog(), builder::setCatalog);
        setOptionalString(value.getSchema(), builder::setSchema);
        setOptionalString(value.getTraceToken(), builder::setTraceToken);
        setOptionalString(value.getRemoteUserAddress(), builder::setRemoteUserAddress);
        setOptionalString(value.getUserAgent(), builder::setUserAgent);
        setOptionalString(value.getClientInfo(), builder::setClientInfo);
        setOptionalString(value.getSelectedUser(), builder::setSelectedUser);
        setOptionalString(value.getReasonForSelect(), builder::setReasonForSelect);

        return builder.build();
    }

    @Override
    public SessionRepresentation fromProtocol(com.facebook.presto.protocol.v2.SessionRepresentation value)
    {
        throw new UnsupportedOperationException("SessionRepresentation proto-to-Java conversion is not implemented yet");
    }

    private static StringMap toStringMap(Map<String, String> value)
    {
        return StringMap.newBuilder()
                .putAllEntries(value)
                .build();
    }

    private static com.facebook.presto.protocol.v2.SelectedRole toProtocol(SelectedRole value)
    {
        com.facebook.presto.protocol.v2.SelectedRole.Builder builder = com.facebook.presto.protocol.v2.SelectedRole.newBuilder()
                .setType(toProtocol(value.getType()));
        value.getRole().ifPresent(builder::setRole);
        return builder.build();
    }

    private static SelectedRoleType toProtocol(SelectedRole.Type value)
    {
        switch (value) {
            case ROLE:
                return SelectedRoleType.SELECTED_ROLE_TYPE_ROLE;
            case ALL:
                return SelectedRoleType.SELECTED_ROLE_TYPE_ALL;
            case NONE:
                return SelectedRoleType.SELECTED_ROLE_TYPE_NONE;
        }
        return SelectedRoleType.SELECTED_ROLE_TYPE_UNKNOWN;
    }
}
