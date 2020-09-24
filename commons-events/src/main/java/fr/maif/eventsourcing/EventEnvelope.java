package fr.maif.eventsourcing;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public class EventEnvelope<E extends Event, Meta, Context> {

    public final UUID id;
    public final Long sequenceNum;
    public final String eventType;
    public final LocalDateTime emissionDate;
    public final String transactionId;
    public final Meta metadata;
    public final E event;
    public final Context context;
    public final Long version;
    public final Boolean published;
    public final Integer totalMessageInTransaction;
    public final Integer numMessageInTransaction;

    public final String entityId;
    public final String userId;
    public final String systemId;

    public EventEnvelope(UUID id, Long sequenceNum, String eventType, LocalDateTime emissionDate, String transactionId, Meta metadata, E event, Context context, Long version, Boolean published, Integer totalMessageInTransaction, Integer numMessageInTransaction, String entityId, String userId, String systemId) {
        this.id = id;
        this.sequenceNum = sequenceNum;
        this.eventType = eventType;
        this.emissionDate = emissionDate;
        this.transactionId = transactionId;
        this.metadata = metadata;
        this.event = event;
        this.context = context;
        this.version = version;
        this.published = published;
        this.totalMessageInTransaction = totalMessageInTransaction;
        this.numMessageInTransaction = numMessageInTransaction;
        this.entityId = entityId;
        this.userId = userId;
        this.systemId = systemId;
    }

    private EventEnvelope(EventEnvelope.Builder<E, Meta, Context> builder) {
        this.id = builder.id;
        this.entityId = builder.entityId;
        this.sequenceNum = builder.sequenceNum;
        this.eventType = builder.eventType;
        this.emissionDate = builder.emissionDate;
        this.userId = builder.userId;
        this.systemId = builder.systemId;
        this.transactionId = builder.transactionId;
        this.metadata = builder.metadata;
        this.event = builder.event;
        this.context = builder.context;
        this.version = builder.version;
        this.published = builder.published;
        this.totalMessageInTransaction = builder.totalMessageInTransaction;
        this.numMessageInTransaction = builder.numMessageInTransaction;
    }

    public static <E extends Event, Meta, Context> Builder<E, Meta, Context> builder() {
        return new Builder<>();
    }

    public static <E extends Event, Meta, Context> Builder<E, Meta, Context> builder(EventEnvelope<E, Meta, Context> envelope) {
        return EventEnvelope.<E, Meta, Context>builder()
                .withEntityId(envelope.entityId)
                .withEvent(envelope.event)
                .withEventType(envelope.eventType)
                .withId(envelope.id)
                .withNumMessageInTransaction(envelope.numMessageInTransaction)
                .withSequenceNum(envelope.sequenceNum)
                .withTotalMessageInTransaction(envelope.totalMessageInTransaction)
                .withTransactionId(envelope.transactionId)
                .withVersion(envelope.version)
                .withContext(envelope.context)
                .withMetadata(envelope.metadata)
                .withSystemId(envelope.systemId)
                .withUserId(envelope.userId)
                .withEmissionDate(envelope.emissionDate)
                .withPublished(envelope.published);
    }

    public Builder<E, Meta, Context> copy() {
        return builder(this);
    }

    public E event() {
        return event;
    }

    public <T> Boolean match(Type<T> type) {
        return type.name().equals(this.eventType) && type.version().equals(this.version);
    }

    @Override
    public String toString() {
        return "EventEnvelope{" +
                "id=" + id +
                ", sequenceNum=" + sequenceNum +
                ", eventType='" + eventType + '\'' +
                ", emissionDate=" + emissionDate +
                ", transactionId='" + transactionId + '\'' +
                ", metadata='" + metadata + '\'' +
                ", event='" + event + '\'' +
                ", context='" + context + '\'' +
                ", version=" + version +
                ", published=" + published +
                ", totalMessageInTransaction=" + totalMessageInTransaction +
                ", numMessageInTransaction=" + numMessageInTransaction +
                ", entityId='" + entityId + '\'' +
                ", userId='" + userId + '\'' +
                ", systemId='" + systemId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventEnvelope<?, ?, ?> that = (EventEnvelope<?, ?, ?>) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(sequenceNum, that.sequenceNum) &&
                Objects.equals(eventType, that.eventType) &&
                Objects.equals(emissionDate, that.emissionDate) &&
                Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(metadata, that.metadata) &&
                Objects.equals(event, that.event) &&
                Objects.equals(context, that.context) &&
                Objects.equals(version, that.version) &&
                Objects.equals(published, that.published) &&
                Objects.equals(totalMessageInTransaction, that.totalMessageInTransaction) &&
                Objects.equals(numMessageInTransaction, that.numMessageInTransaction) &&
                Objects.equals(entityId, that.entityId) &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(systemId, that.systemId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, sequenceNum, eventType, emissionDate, transactionId, metadata, event, context, version, published, totalMessageInTransaction, numMessageInTransaction, entityId, userId, systemId);
    }

    public static final class Builder<E extends Event, Meta, Context> {
        public UUID id;
        public Long sequenceNum;
        public String eventType;
        public LocalDateTime emissionDate;
        public String transactionId;
        public Meta metadata;
        public E event;
        public Context context;
        public Long version;
        public Boolean published;
        public String entityId;
        public String userId;
        public String systemId;
        public Integer totalMessageInTransaction;
        public Integer numMessageInTransaction;

        public Builder<E, Meta, Context> withId(UUID id) {
            this.id = id;
            return this;
        }

        public Builder<E, Meta, Context> withSequenceNum(Long sequenceNum) {
            this.sequenceNum = sequenceNum;
            return this;
        }

        public Builder<E, Meta, Context> withEventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder<E, Meta, Context> withEmissionDate(LocalDateTime emissionDate) {
            this.emissionDate = emissionDate;
            return this;
        }

        public Builder<E, Meta, Context> withTransactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder<E, Meta, Context> withMetadata(Meta metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder<E, Meta, Context> withEvent(E event) {
            this.event = event;
            return this;
        }

        public Builder<E, Meta, Context> withContext(Context context) {
            this.context = context;
            return this;
        }

        public Builder<E, Meta, Context> withVersion(Long version) {
            this.version = version;
            return this;
        }

        public Builder<E, Meta, Context> withPublished(Boolean published) {
            this.published = published;
            return this;
        }

        public Builder<E, Meta, Context> withEntityId(String entityId) {
            this.entityId = entityId;
            return this;
        }

        public Builder<E, Meta, Context> withUserId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder<E, Meta, Context> withSystemId(String systemId) {
            this.systemId = systemId;
            return this;
        }

        public Builder<E, Meta, Context> withTotalMessageInTransaction(Integer totalMessageInTransaction) {
            this.totalMessageInTransaction = totalMessageInTransaction;
            return this;
        }

        public Builder<E, Meta, Context> withNumMessageInTransaction(Integer numMessageInTransaction) {
            this.numMessageInTransaction = numMessageInTransaction;
            return this;
        }

        public EventEnvelope<E, Meta, Context> build() {
            return new EventEnvelope<>(this);
        }
    }
}
