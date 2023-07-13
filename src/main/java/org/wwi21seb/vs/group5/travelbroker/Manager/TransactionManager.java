package org.wwi21seb.vs.group5.travelbroker.Manager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class TransactionManager {

    @JsonProperty("transaction_id")
    private UUID transactionId;

    @JsonProperty("resource_id")
    private UUID resourceId;

    @JsonCreator
    public TransactionManager(
            @JsonProperty("transaction_id") UUID transactionId,
            @JsonProperty("resource_id") UUID resourceId) {
        this.transactionId = transactionId;
        this.resourceId = resourceId;
    }

}
