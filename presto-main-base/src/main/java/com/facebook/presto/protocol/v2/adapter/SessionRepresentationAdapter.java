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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.protocol.v2.SelectedRoleType;
import com.facebook.presto.protocol.v2.StringMap;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.protocol.v2.adapter.ProtocolAdapters.setOptionalString;
import static com.facebook.presto.spi.session.ResourceEstimates.CPU_TIME;
import static com.facebook.presto.spi.session.ResourceEstimates.EXECUTION_TIME;
import static com.facebook.presto.spi.session.ResourceEstimates.PEAK_MEMORY;
import static com.facebook.presto.spi.session.ResourceEstimates.PEAK_TASK_MEMORY;
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
                .putAllResourceEstimates(toResourceEstimates(value.getResourceEstimates()))
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
        requireNonNull(value, "value is null");

        if (!value.getSessionFunctionsList().isEmpty()) {
            throw new UnsupportedOperationException("Session function proto-to-Java conversion requires connector payload decoding");
        }

        return new SessionRepresentation(
                value.getQueryId(),
                value.hasTransactionId() ? Optional.of(TransactionId.valueOf(value.getTransactionId())) : Optional.empty(),
                value.getClientTransactionSupport(),
                value.getUser(),
                optionalString(value.hasPrincipal(), value.getPrincipal()),
                optionalString(value.hasSource(), value.getSource()),
                optionalString(value.hasCatalog(), value.getCatalog()),
                optionalString(value.hasSchema(), value.getSchema()),
                optionalString(value.hasTraceToken(), value.getTraceToken()),
                TimeZoneKey.getTimeZoneKey((short) value.getTimeZoneKey()),
                Locale.forLanguageTag(value.getLocale()),
                optionalString(value.hasRemoteUserAddress(), value.getRemoteUserAddress()),
                optionalString(value.hasUserAgent(), value.getUserAgent()),
                optionalString(value.hasClientInfo(), value.getClientInfo()),
                ImmutableSet.copyOf(value.getClientTagsList()),
                fromResourceEstimates(value.getResourceEstimatesMap()),
                value.getStartTime(),
                ImmutableMap.copyOf(value.getSystemPropertiesMap()),
                value.getCatalogPropertiesMap().entrySet().stream()
                        .collect(toMap(entry -> new ConnectorId(entry.getKey()), entry -> ImmutableMap.copyOf(entry.getValue().getEntriesMap()))),
                value.getUnprocessedCatalogPropertiesMap().entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue().getEntriesMap()))),
                value.getRolesMap().entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, entry -> fromProtocol(entry.getValue()))),
                ImmutableMap.copyOf(value.getPreparedStatementsMap()),
                ImmutableMap.of(),
                optionalString(value.hasSelectedUser(), value.getSelectedUser()),
                optionalString(value.hasReasonForSelect(), value.getReasonForSelect()));
    }

    private static Optional<String> optionalString(boolean present, String value)
    {
        return present ? Optional.of(value) : Optional.empty();
    }

    private static Map<String, String> toResourceEstimates(ResourceEstimates value)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        value.getExecutionTime().ifPresent(duration -> builder.put(EXECUTION_TIME, duration.toString()));
        value.getCpuTime().ifPresent(duration -> builder.put(CPU_TIME, duration.toString()));
        value.getPeakMemory().ifPresent(dataSize -> builder.put(PEAK_MEMORY, dataSize.toString()));
        value.getPeakTaskMemory().ifPresent(dataSize -> builder.put(PEAK_TASK_MEMORY, dataSize.toString()));
        return builder.build();
    }

    private static ResourceEstimates fromResourceEstimates(Map<String, String> value)
    {
        Optional<Duration> executionTime = Optional.empty();
        Optional<Duration> cpuTime = Optional.empty();
        Optional<DataSize> peakMemory = Optional.empty();
        Optional<DataSize> peakTaskMemory = Optional.empty();

        for (Map.Entry<String, String> entry : value.entrySet()) {
            switch (entry.getKey().toUpperCase()) {
                case EXECUTION_TIME:
                    executionTime = Optional.of(Duration.valueOf(entry.getValue()));
                    break;
                case CPU_TIME:
                    cpuTime = Optional.of(Duration.valueOf(entry.getValue()));
                    break;
                case PEAK_MEMORY:
                    peakMemory = Optional.of(DataSize.valueOf(entry.getValue()));
                    break;
                case PEAK_TASK_MEMORY:
                    peakTaskMemory = Optional.of(DataSize.valueOf(entry.getValue()));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported resource estimate: " + entry.getKey());
            }
        }

        return new ResourceEstimates(executionTime, cpuTime, peakMemory, peakTaskMemory);
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

    private static SelectedRole fromProtocol(com.facebook.presto.protocol.v2.SelectedRole value)
    {
        return new SelectedRole(fromProtocol(value.getType()), optionalString(value.hasRole(), value.getRole()));
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

    private static SelectedRole.Type fromProtocol(SelectedRoleType value)
    {
        switch (value) {
            case SELECTED_ROLE_TYPE_ROLE:
                return SelectedRole.Type.ROLE;
            case SELECTED_ROLE_TYPE_ALL:
                return SelectedRole.Type.ALL;
            case SELECTED_ROLE_TYPE_NONE:
                return SelectedRole.Type.NONE;
            case SELECTED_ROLE_TYPE_UNKNOWN:
            case UNRECOGNIZED:
        }
        throw new IllegalArgumentException("Unsupported selected role type: " + value);
    }
}
