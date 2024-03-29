package com.example.demo;

import com.fasterxml.jackson.databind.JsonNode;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.json.Json;
import fr.maif.json.JsonWrite;
import io.vavr.API;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Either;

import static com.example.demo.BankEvent.AccountClosedV1;
import static com.example.demo.BankEvent.AccountOpenedV1;
import static com.example.demo.BankEvent.MoneyDepositedV1;
import static com.example.demo.BankEvent.MoneyWithdrawnV1;
import static io.vavr.API.Case;
import static io.vavr.API.Left;

public class BankEventFormat implements JacksonEventFormat<String, BankEvent> {

    @Override
    public Either<String, BankEvent> read(String type, Long version, JsonNode json) {
        return Either.narrow(switch (Tuple.of(type, version)) {
            case Tuple2<String, Long> t && MoneyDepositedV1.match(t) ->
                    Json.fromJson(json, BankEvent.MoneyDeposited.class).toEither().mapLeft(errs -> errs.mkString(","));
            case Tuple2<String, Long> t && MoneyWithdrawnV1.match(t) ->
                    Json.fromJson(json, BankEvent.MoneyWithdrawn.class).toEither().mapLeft(errs -> errs.mkString(","));
            case Tuple2<String, Long> t && AccountClosedV1.match(t) ->
                    Json.fromJson(json, BankEvent.AccountClosed.class).toEither().mapLeft(errs -> errs.mkString(","));
            case Tuple2<String, Long> t && AccountOpenedV1.match(t) ->
                    Json.fromJson(json, BankEvent.AccountOpened.class).toEither().mapLeft(errs -> errs.mkString(","));
            default -> Either.<String, BankEvent>left("Unknown event type " + type + "(v" + version + ")");
        });
    }

    @Override
    public JsonNode write(BankEvent event) {
        return Json.toJson(event, JsonWrite.auto());
    }
}
