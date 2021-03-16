package fr.maif.thoth.sample.state;

import java.math.BigDecimal;

import fr.maif.eventsourcing.AbstractState;
import lombok.Builder;

@Builder(toBuilder = true)
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
