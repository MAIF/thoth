package com.example.demo;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Type;
import fr.maif.json.Json;
import fr.maif.json.JsonFormat;
import fr.maif.json.JsonRead;

import java.math.BigDecimal;

import static fr.maif.json.Json.$$;
import static fr.maif.json.JsonRead.__;
import static fr.maif.json.JsonRead._bigDecimal;
import static fr.maif.json.JsonRead._string;
import static fr.maif.json.JsonRead.caseOf;
import static fr.maif.json.JsonWrite.$bigdecimal;

public sealed interface BankEvent extends Event {

    Type<MoneyWithdrawn> MoneyWithdrawnV1 = Type.create(MoneyWithdrawn.class, 1L);
    Type<AccountOpened> AccountOpenedV1 = Type.create(AccountOpened.class, 1L);
    Type<MoneyDeposited> MoneyDepositedV1 = Type.create(MoneyDeposited.class, 1L);
    Type<AccountClosed> AccountClosedV1 = Type.create(AccountClosed.class, 1L);

    JsonFormat<BankEvent> format = JsonFormat.of(
            JsonRead.oneOf(_string("type"),
                    caseOf("MoneyWithdrawn"::equals, MoneyWithdrawn.format),
                    caseOf("AccountOpened"::equals, AccountOpened.format),
                    caseOf("MoneyDeposited"::equals, MoneyDeposited.format),
                    caseOf("AccountClosed"::equals, AccountClosed.format)
            ),
            (BankEvent event) -> switch (event) {
                    case MoneyWithdrawn bankEvent -> MoneyWithdrawn.format.write(bankEvent);
                    case AccountOpened bankEvent -> AccountOpened.format.write(bankEvent);
                    case MoneyDeposited bankEvent -> MoneyDeposited.format.write(bankEvent);
                    case AccountClosed bankEvent -> AccountClosed.format.write(bankEvent);
            }
    );

    record MoneyWithdrawn(String accountId, BigDecimal amount) implements BankEvent {

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

    record AccountOpened(String accountId) implements BankEvent {
        static class AccountOpenedBuilder {
            String accountId;

            AccountOpenedBuilder accountId(String accountId){
                this.accountId = accountId;
                return this;
            }

            AccountOpened build(){
                return new AccountOpened(accountId);
            }
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


    record MoneyDeposited(String accountId, BigDecimal amount) implements BankEvent {

        static class MoneyDepositedBuilder {
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

    record AccountClosed(String accountId) implements BankEvent {
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
