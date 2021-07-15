package com.example.demo;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Type;
import fr.maif.json.Json;
import fr.maif.json.JsonFormat;
import fr.maif.json.JsonRead;
import io.vavr.API.Match.Pattern0;

import java.math.BigDecimal;

import static fr.maif.json.Json.$$;
import static fr.maif.json.JsonRead.__;
import static fr.maif.json.JsonRead._bigDecimal;
import static fr.maif.json.JsonRead._string;
import static fr.maif.json.JsonRead.caseOf;
import static fr.maif.json.JsonWrite.$bigdecimal;
import static io.vavr.API.Case;
import static io.vavr.API.Match;

public interface BankEvent extends Event {

    Type<MoneyWithdrawn> MoneyWithdrawnV1 = Type.create(MoneyWithdrawn.class, 1L);
    Type<AccountOpened> AccountOpenedV1 = Type.create(AccountOpened.class, 1L);
    Type<MoneyDeposited> MoneyDepositedV1 = Type.create(MoneyDeposited.class, 1L);
    Type<AccountClosed> AccountClosedV1 = Type.create(AccountClosed.class, 1L);

    static Pattern0<MoneyWithdrawn> $MoneyWithdrawn() {
        return Pattern0.of(MoneyWithdrawn.class);
    }
    static Pattern0<AccountOpened> $AccountOpened() {
        return Pattern0.of(AccountOpened.class);
    }
    static Pattern0<MoneyDeposited> $MoneyDeposited() {
        return Pattern0.of(MoneyDeposited.class);
    }
    static Pattern0<AccountClosed> $AccountClosed() {
        return Pattern0.of(AccountClosed.class);
    }

    JsonFormat<BankEvent> format = JsonFormat.of(
            JsonRead.oneOf(_string("type"),
                    caseOf("MoneyWithdrawn"::equals, MoneyWithdrawn.format),
                    caseOf("AccountOpened"::equals, AccountOpened.format),
                    caseOf("MoneyDeposited"::equals, MoneyDeposited.format),
                    caseOf("AccountClosed"::equals, AccountClosed.format)
            ),
            (BankEvent event) -> Match(event).of(
                    Case($MoneyWithdrawn(), MoneyWithdrawn.format::write),
                    Case($AccountOpened(), AccountOpened.format::write),
                    Case($MoneyDeposited(), MoneyDeposited.format::write),
                    Case($AccountClosed(), AccountClosed.format::write)
            )
    );

    class MoneyWithdrawn implements BankEvent {
        public final String accountId;
        public final BigDecimal amount;

        static class MoneyWithdrawnBuilder{
            String accountId;
            BigDecimal amount;

            MoneyWithdrawnBuilder accountId(String accountId){
                this.accountId = accountId;
                return this;
            }

            MoneyWithdrawnBuilder amount(BigDecimal amount){
                this.amount = amount;
                return this;
            }

            MoneyWithdrawn build(){
                return new MoneyWithdrawn(accountId,amount);
            }

        }

        public MoneyWithdrawn(String accountId, BigDecimal amount) {
            this.accountId = accountId;
            this.amount = amount;
        }

        public static MoneyWithdrawnBuilder builder(){
            return new MoneyWithdrawnBuilder();
        }

        @Override
        public Type<MoneyWithdrawn> type() {
            return MoneyWithdrawnV1;
        }

        @Override
        public String entityId() {
            return accountId;
        }

        public static JsonFormat<MoneyWithdrawn> format = JsonFormat.of(
                __("amount", _bigDecimal(), MoneyWithdrawn.builder()::amount)
                        .and(_string("accountId"), MoneyWithdrawn.MoneyWithdrawnBuilder::accountId)
                        .map(MoneyWithdrawn.MoneyWithdrawnBuilder::build),
                (MoneyWithdrawn moneyWithdrawn) -> Json.obj(
                        $$("type", "MoneyWithdrawn"),
                        $$("amount", moneyWithdrawn.amount, $bigdecimal()),
                        $$("accountId", moneyWithdrawn.accountId)
                )
        );
    }

    class AccountOpened implements BankEvent {
        public final String accountId;

        static class AccountOpenedBuilder{
            String accountId;

            AccountOpenedBuilder accountId(String accountId){
                this.accountId = accountId;
                return this;
            }

            AccountOpened build(){
                return new AccountOpened(accountId);
            }
        }

        public AccountOpened(String accountId) {
            this.accountId = accountId;
        }

        public static AccountOpenedBuilder builder(){
            return new AccountOpenedBuilder();
        }

        @Override
        public Type<AccountOpened> type() {
            return AccountOpenedV1;
        }

        @Override
        public String entityId() {
            return accountId;
        }

        public static JsonFormat<AccountOpened> format = JsonFormat.of(
                __("accountId", _string(), AccountOpened.AccountOpened.builder()::accountId)
                        .map(AccountOpened.AccountOpenedBuilder::build),
                (AccountOpened accountOpened) -> Json.obj(
                    $$("type", "AccountOpened"),
                    $$("accountId", accountOpened.accountId)
                )
        );
    }


    class MoneyDeposited implements BankEvent {
        public final String accountId;
        public final BigDecimal amount;

        public MoneyDeposited(String accountId, BigDecimal amount) {
            this.accountId = accountId;
            this.amount = amount;
        }

        static class MoneyDepositedBuilder{
            String accountId;
            BigDecimal amount;

            MoneyDepositedBuilder accountId(String accountId){
                this.accountId = accountId;
                return this;
            }

            MoneyDepositedBuilder amount(BigDecimal amount){
                this.amount = amount;
                return this;
            }

            MoneyDeposited build(){
                return new MoneyDeposited(accountId,amount);
            }

        }

        public static MoneyDepositedBuilder builder(){
            return new MoneyDepositedBuilder();
        }

        @Override
        public Type<MoneyDeposited> type() {
            return MoneyDepositedV1;
        }

        @Override
        public String entityId() {
            return accountId;
        }

        public static JsonFormat<MoneyDeposited> format = JsonFormat.of(
                __("accountId", _string(), MoneyDeposited.MoneyDeposited.builder()::accountId)
                        .and(__("amount", _bigDecimal()), MoneyDeposited.MoneyDepositedBuilder::amount)
                        .map(MoneyDeposited.MoneyDepositedBuilder::build),
                (MoneyDeposited moneyDeposited) -> Json.obj(
                        $$("type", "MoneyDeposited"),
                        $$("amount", moneyDeposited.amount, $bigdecimal()),
                        $$("accountId", moneyDeposited.accountId)
                )
        );
    }

    class AccountClosed implements BankEvent {
        public final String accountId;

        static class AccountClosedBuilder{
            String accountId;

            AccountClosedBuilder accountId(String accountId){
                this.accountId = accountId;
                return this;
            }

            AccountClosed build(){
                return new AccountClosed(accountId);
            }
        }

        public AccountClosed(String accountId) {
            this.accountId = accountId;
        }

        public static AccountClosedBuilder builder(){
            return new AccountClosedBuilder();
        }

        @Override
        public Type<AccountClosed> type() {
            return AccountClosedV1;
        }

        @Override
        public String entityId() {
            return accountId;
        }


        public static JsonFormat<AccountClosed> format = JsonFormat.of(
                __("accountId", _string(), AccountClosed.AccountClosed.builder()::accountId)
                        .map(AccountClosed.AccountClosedBuilder::build),
                (AccountClosed accountClosed) -> Json.obj(
                        $$("type", "AccountClosed"),
                        $$("accountId", accountClosed.accountId)
                )
        );
    }
}
