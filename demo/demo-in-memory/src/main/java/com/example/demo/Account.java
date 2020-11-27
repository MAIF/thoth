package com.example.demo;

import fr.maif.eventsourcing.AbstractState;

import java.math.BigDecimal;

public class Account extends AbstractState<Account> {
    public String id;
    public BigDecimal balance;
    public long sequenceNum;

    @Override
    public Long sequenceNum() {
        return sequenceNum;
    }

    @Override
    public Account withSequenceNum(Long sequenceNum) {
        this.sequenceNum = sequenceNum;
        return this;
    }
}
