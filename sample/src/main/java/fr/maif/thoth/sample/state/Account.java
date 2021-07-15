package fr.maif.thoth.sample.state;

import java.math.BigDecimal;

import fr.maif.eventsourcing.AbstractState;

public class Account extends AbstractState<Account> {
    public String id;
    public BigDecimal balance;
    public long sequenceNum;

    public static class AccountBuilder{
        String id;
        BigDecimal balance;
        long sequenceNum;

        public AccountBuilder() {
        }

        public AccountBuilder(String id, BigDecimal balance, long sequenceNum) {
            this.id = id;
            this.balance = balance;
            this.sequenceNum = sequenceNum;
        }

        public AccountBuilder id(String id){
            this.id = id;
            return this;
        }

        public AccountBuilder balance(BigDecimal balance){
            this.balance = balance;
            return this;
        }

        public AccountBuilder withSequenceNum(long sequenceNum){
            this.sequenceNum = sequenceNum;
            return this;
        }

        public Account build(){
            return new Account(this.id,this.balance,this.sequenceNum);
        }

    }

    public Account(String id, BigDecimal balance, long sequenceNum) {
        this.id = id;
        this.balance = balance;
        this.sequenceNum = sequenceNum;
    }

    public AccountBuilder toBuilder(){
        return new AccountBuilder(this.id, this.balance, this.sequenceNum);
    }

    public static AccountBuilder builder(){
        return new AccountBuilder();
    }

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
