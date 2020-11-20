package com.example.demo;

import fr.maif.eventsourcing.State;
import lombok.Getter;
import lombok.ToString;

import java.math.BigDecimal;

@Getter
@ToString
public class Account implements State<Account> {
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
