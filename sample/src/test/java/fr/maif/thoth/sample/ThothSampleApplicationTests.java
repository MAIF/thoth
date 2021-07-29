package fr.maif.thoth.sample;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.HttpClientErrorException;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import akka.actor.ActorSystem;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessor;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.json.EventEnvelopeJson;
import fr.maif.thoth.sample.api.AccountDTO;
import fr.maif.thoth.sample.api.BalanceDTO;
import fr.maif.thoth.sample.api.TransferDTO;
import fr.maif.thoth.sample.api.TransferResultDTO;
import fr.maif.thoth.sample.commands.BankCommand;
import fr.maif.thoth.sample.events.BankEvent;
import fr.maif.thoth.sample.events.BankEventFormat;
import fr.maif.thoth.sample.projections.transactional.GlobalBalanceProjection;
import fr.maif.thoth.sample.state.Account;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.Tuple2;
import org.testcontainers.utility.DockerImageName;

@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers
class ThothSampleApplicationTests {
	private static final String groupId = "test-groupid";
	@Autowired
	private TestRestTemplate restTemplate;
	@Autowired
	private ObjectMapper mapper;
	@Autowired
	private BankEventFormat eventFormat;
	@Autowired
	private GlobalBalanceProjection projection;
	@Autowired
	private EventProcessor<String, Account, BankCommand, BankEvent, Connection, Tuple0, Tuple0, Tuple0> eventProcessor;
	@Autowired
	private ActorSystem actorSystem;

	@Container
	private static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres").withTag("11.2"));
	@Container
	private static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("6.0.1"));
	private PGSimpleDataSource pgDataSource;

	static {
		postgres.start();
		kafka.start();
	}


	@DynamicPropertySource
	static void databaseProperties(DynamicPropertyRegistry registry) {
		registry.add("db.host", postgres::getHost);
		registry.add("db.user", postgres::getUsername);
		registry.add("db.password", postgres::getPassword);
		registry.add("db.port", postgres::getFirstMappedPort);
		registry.add("db.name", postgres::getDatabaseName);
		registry.add("kafka.host", kafka::getHost);
		registry.add("kafka.port", kafka::getFirstMappedPort);
	}

	@BeforeAll
	public void initTest() {
		this.pgDataSource = new PGSimpleDataSource();
		pgDataSource.setUrl(postgres.getJdbcUrl());
		pgDataSource.setUser(postgres.getUsername());
		pgDataSource.setPassword(postgres.getPassword());
		// Override default setting, which wait indefinitely if database is down
		pgDataSource.setLoginTimeout(5);
	}

	@AfterEach
	public void clearStuff() throws SQLException {
		pgDataSource.getConnection().prepareStatement("TRUNCATE global_balance;").execute();
		pgDataSource.getConnection().prepareStatement("INSERT INTO global_balance (balance) VALUES(0::money) ON CONFLICT DO NOTHING;").execute();
		pgDataSource.getConnection().prepareStatement("TRUNCATE withdraw_by_month;").execute();
		pgDataSource.getConnection().prepareStatement("TRUNCATE bank_journal;").execute();
		consumePublishedEvents();
	}

	@AfterAll
	public void tearDown() {
		postgres.stop();
		kafka.stop();
	}

	@Test
	void postAccountShouldCreateAccount() {
		final String id = "test";
		AccountDTO payload = new AccountDTO(new BigDecimal("100"), id);

		assertThat(createAccount(payload)).isEqualTo(payload);

		assertThat(readAccount(id)).isEqualTo(payload);

		assertThat(consumePublishedEvents()).hasSize(2);
	}

	@Test
	void deleteAccountShouldDeleteAccount() {
		final String id = "test";
		AccountDTO payload = new AccountDTO(new BigDecimal("100"), id);

		createAccount(payload);
		closeAccount(id);
		accountShouldNotExist(id);

		assertThat(consumePublishedEvents()).hasSize(3);
	}

	@Test
	void withdraw() {
		String id = "test";
		AccountDTO payload = new AccountDTO(new BigDecimal("100"), id);

		createAccount(payload);

		AccountDTO expected = new AccountDTO(new BigDecimal("90"), id);
		assertThat(withdraw(id, new BigDecimal("10"))).isEqualTo(expected);
		assertThat(readAccount(id)).isEqualTo(expected);

		assertThat(consumePublishedEvents()).hasSize(3);
	}

	@Test
	void deposit() {
		String id = "test";
		AccountDTO payload = new AccountDTO(new BigDecimal("100"), id);

		createAccount(payload);

		AccountDTO expected = new AccountDTO(new BigDecimal("110"), id);
		assertThat(deposit(id, new BigDecimal("10"))).isEqualTo(expected);
		assertThat(readAccount(id)).isEqualTo(expected);

		assertThat(consumePublishedEvents()).hasSize(3);
	}

	@Test
	void withdrawShouldFailOnInsufficientBalance() {
		String id = "test";

		AccountDTO payload = new AccountDTO(new BigDecimal("100"), id);
		createAccount(payload);
		AccountDTO expected = AccountDTO.error("Overdrawn account");
		assertThat(withdraw(id, new BigDecimal("110"))).isEqualTo(expected);
		assertThat(readAccount(id)).isEqualTo(payload);

		assertThat(consumePublishedEvents()).hasSize(2);
	}

	@Test
	void withdrawShouldFailOnNonExistantAccount() {
		String id = "test";

		AccountDTO expected = AccountDTO.error("Account does not exist");
		assertThat(withdraw(id, new BigDecimal("110"))).isEqualTo(expected);
		accountShouldNotExist(id);

		assertThat(consumePublishedEvents()).hasSize(0);
	}

	@Test
	void depositShouldFailOnNonExistantAccount() {
		String id = "test";

		AccountDTO expected = AccountDTO.error("Account does not exist");
		assertThat(deposit(id, new BigDecimal("110"))).isEqualTo(expected);
		accountShouldNotExist(id);

		assertThat(consumePublishedEvents()).hasSize(0);
	}

	@Test
	void creationShouldFailOnNegativeAmount() {
		final String id = "test";
		AccountDTO payload = new AccountDTO(new BigDecimal("-100"), id);

		assertThat(createAccount(payload).error).isEqualTo("Initial balance can't be negative");

		accountShouldNotExist(id);

		assertThat(consumePublishedEvents()).hasSize(0);
	}

	@Test
	void creationShouldFailOnExistantAccount() {
		final String id = "test";
		AccountDTO payload = new AccountDTO(new BigDecimal("100"), id);
		createAccount(payload);
		assertThat(createAccount(payload).error).isEqualTo("An account already exists for the id: test");

		assertThat(consumePublishedEvents()).hasSize(2);
	}

	@Test
	void deleteAccountShouldFailOnNonExistantAccount() {
		final String id = "test";
		final Optional<String> maybeError = closeAccount(id);
		assertThat(maybeError.get()).isEqualTo("Account does not exist");
		accountShouldNotExist(id);

		assertThat(consumePublishedEvents()).hasSize(0);
	}

	@Test
	void globalBalanceShouldBeAccurate() {
		createAccount(new AccountDTO(new BigDecimal("100"), "test"));
		createAccount(new AccountDTO(new BigDecimal("200"), "test2"));
		deposit("test", new BigDecimal("50"));
		withdraw("test", new BigDecimal("25"));

		assertThat(globalBalance().amount).isEqualByComparingTo("325");
	}

	@Test
	void meanWithdrawByMonthShouldBeAccurate() throws InterruptedException {
		final String id = "test";
		createAccount(new AccountDTO(new BigDecimal("1000"), id));
		withdraw(id, new BigDecimal("100"));
		withdraw(id, new BigDecimal("200"));
		withdraw(id, new BigDecimal("600"));

		// Wait for kafka consumption
		Thread.sleep(1000);

		final BalanceDTO response = meanWithdrawByMonth(id, LocalDate.now().getMonth().name(), LocalDate.now().getYear());
		assertThat(response.amount).isEqualByComparingTo("300");
	}

	@Test
	void meanWithdrawShouldBeAccurate() throws InterruptedException {
		final String id = "test";
		createAccount(new AccountDTO(new BigDecimal("1000"), id));
		withdraw(id, new BigDecimal("100"));
		withdraw(id, new BigDecimal("200"));
		withdraw(id, new BigDecimal("600"));

		// Wait for kafka consumption
		Thread.sleep(1000);

		final BalanceDTO response = meanWithdraw(id);
		assertThat(response.amount).isEqualByComparingTo("300");
	}


	@Test
	void transferShouldWork() {
		final String sourceId = "from";
		final String targetId = "to";

		createAccount(new AccountDTO(new BigDecimal("1000"), sourceId));
		createAccount(new AccountDTO(new BigDecimal("10"), targetId));
		final TransferResultDTO result = transfer(sourceId, targetId, new BigDecimal("100"));

		assertThat(result.from.balance).isEqualByComparingTo("900");
		assertThat(result.to.balance).isEqualByComparingTo("110");
	}

	@Test
	void transferShouldFailIfBalanceIsTooLow() {
		final String sourceId = "from";
		final String targetId = "to";

		createAccount(new AccountDTO(new BigDecimal("50"), sourceId));
		createAccount(new AccountDTO(new BigDecimal("10"), targetId));
		final TransferResultDTO result = transfer(sourceId, targetId, new BigDecimal("100"));

		assertThat(result.error).isEqualTo("Overdrawn account");
	}

	@Test
	void transferShouldFailIfSourceAccountDoesNotExist() {
		final String targetId = "to";

		createAccount(new AccountDTO(new BigDecimal("10"), targetId));
		final TransferResultDTO result = transfer("from", targetId, new BigDecimal("100"));

		assertThat(result.error).isEqualTo("Account does not exist");
	}

	@Test
	void transferShouldFailIfTargetAccountDoesNotExist() {
		final String sourceId = "from";

		createAccount(new AccountDTO(new BigDecimal("1000"), sourceId));
		final TransferResultDTO result = transfer(sourceId, "to", new BigDecimal("100"));

		assertThat(result.error).isEqualTo("Account does not exist");
	}

	@Test
	void projectionShouldCatchUpExistingEvents() throws SQLException {
		String entityId = "test";

		registerInJournal(new BankEvent.AccountOpened(entityId));
		registerInJournal(new BankEvent.MoneyDeposited(entityId, new BigDecimal("100")));
		registerInJournal(new BankEvent.MoneyWithdrawn(entityId, new BigDecimal("30")));

		assertThat(globalBalance().amount).isEqualByComparingTo(new BigDecimal("0"));

		projection.initialize(pgDataSource.getConnection(), eventProcessor.eventStore(), actorSystem).join();

		assertThat(globalBalance().amount).isEqualByComparingTo(new BigDecimal("70"));

	}

	private void registerInJournal(BankEvent event) throws SQLException {
		String eventType;
		String json;
		if(event instanceof BankEvent.AccountOpened) {
			eventType = "AccountOpened";
			json = "{\"accountId\": \"" + event.accountId + "\"}";
		} else if(event instanceof BankEvent.MoneyWithdrawn withdraw) {
			eventType = "MoneyWithdrawn";
			json = "{\"amount\": " + withdraw.amount + ", \"accountId\": \"" + event.accountId + "\"}";
		} else if(event instanceof BankEvent.MoneyDeposited deposit) {
			eventType = "MoneyDeposited";
			json = "{\"amount\": " + deposit.amount + ", \"accountId\": \"" + event.accountId + "\"}";
		} else if(event instanceof BankEvent.AccountClosed) {
			eventType = "AccountClosed";
			json = "{\"accountId\": \"" + event.accountId + "\"}";
		} else {
			throw new RuntimeException("Unknown event type " + event.getClass().getSimpleName());
		}


		try(Connection connection = pgDataSource.getConnection()) {
			final ResultSet sequenceResult = connection.prepareStatement("select nextval('bank_sequence_num')").executeQuery();
			sequenceResult.next();
			final long sequenceNum = sequenceResult.getLong("nextval");
			final PreparedStatement statement = connection.prepareStatement("""
					INSERT INTO bank_journal (id, entity_id, sequence_num, event_type, version, transaction_id, event, total_message_in_transaction, num_message_in_transaction, emission_date, published)
					VALUES(?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?);
					""");

			statement.setObject(1, UUID.randomUUID());
			statement.setString(2, event.entityId());
			statement.setLong(3, sequenceNum);
			statement.setString(4, eventType);
			statement.setInt(5, 1);
			statement.setObject(6, UUID.randomUUID());
			statement.setObject(7, json);
			statement.setInt(8, 1);
			statement.setInt(9, 1);
			statement.setDate(10, new Date(System.currentTimeMillis()));
			statement.setBoolean(11, false);

			statement.execute();
		}
	}



	private AccountDTO readAccount(String id) {
		try {
			return restTemplate.getForEntity("/bank/api/" + id, AccountDTO.class).getBody();
		} catch(HttpClientErrorException ex) {
			try {
				return mapper.readValue(ex.getResponseBodyAsString(), AccountDTO.class);
			} catch (IOException e) {
				throw ex;
			}
		}
	}

	private AccountDTO createAccount(AccountDTO account) {
		try {
			return restTemplate.postForEntity("/bank/api/", account, AccountDTO.class).getBody();
		} catch(HttpClientErrorException ex) {
			try {
				return mapper.readValue(ex.getResponseBodyAsString(), AccountDTO.class);
			} catch (IOException e) {
				throw ex;
			}
		}
	}

	private Optional<String> closeAccount(String id) {
		try {
			final String body = restTemplate.exchange("/bank/api/" + id, HttpMethod.DELETE, null, String.class).getBody();
			if(Objects.isNull(body)) {
				return Optional.empty();
			} else {
				try {
					return Optional.ofNullable(mapper.readValue(body, AccountDTO.class).error);
				} catch (JsonProcessingException e) {
					return Optional.empty();
				}
			}
		} catch(HttpClientErrorException ex) {
			try {
				return Optional.ofNullable(mapper.readValue(ex.getResponseBodyAsString(), AccountDTO.class).error);
			} catch (IOException e) {
				throw ex;
			}
		}
	}

	private AccountDTO withdraw(String id, BigDecimal amount) {
		BalanceDTO request = new BalanceDTO();
		request.amount = amount;
		try {
			return restTemplate.postForEntity("/bank/api/" + id + "/_action/withdraw", request, AccountDTO.class).getBody();
		} catch(HttpClientErrorException ex) {
			try {
				return mapper.readValue(ex.getResponseBodyAsString(), AccountDTO.class);
			} catch (IOException e) {
				throw ex;
			}
		}
	}

	private AccountDTO deposit(String id, BigDecimal amount) {
		BalanceDTO request = new BalanceDTO();
		request.amount = amount;
		try {
			return restTemplate.postForEntity("/bank/api/" + id + "/_action/deposit", request, AccountDTO.class).getBody();
		} catch(HttpClientErrorException ex) {
			try {
				return mapper.readValue(ex.getResponseBodyAsString(), AccountDTO.class);
			} catch (IOException e) {
				throw ex;
			}
		}
	}


	private TransferResultDTO transfer(String source, String target, BigDecimal amount) {
		TransferDTO request = new TransferDTO();
		request.amount = amount;
		request.from = source;
		request.to = target;

		try {
			return restTemplate.postForEntity("/bank/api/_action/transfer", request, TransferResultDTO.class).getBody();
		} catch(HttpClientErrorException ex) {
			try {
				return mapper.readValue(ex.getResponseBodyAsString(), TransferResultDTO.class);
			} catch (IOException e) {
				throw ex;
			}
		}
	}

	private BalanceDTO globalBalance() {
		return restTemplate.getForEntity("/bank/api/balance", BalanceDTO.class).getBody();
	}

	private BalanceDTO meanWithdrawByMonth(String id, String month, int year) {
		return restTemplate.getForEntity("/bank/api/stats/" + id + "?month=" + month + "&year" + year, BalanceDTO.class).getBody();
	}

	private BalanceDTO meanWithdraw(String id) {
		return restTemplate.getForEntity("/bank/api/stats/" + id, BalanceDTO.class).getBody();
	}

	private void accountShouldNotExist(String id) {
		assertThat(readAccount(id).error).isEqualTo("Account does not exist");
	}


	public List<EventEnvelope<BankEvent, Tuple0, Tuple0>> consumePublishedEvents() {

		Optional<Long> maybeLastOffset =  getEndOffsetIfNotReached("bank", kafka.getBootstrapServers());
		if(maybeLastOffset.isEmpty()) {
			return Collections.emptyList();
		}
		long lastOffset = maybeLastOffset.get();

		Properties props = new Properties();
		props.put("bootstrap.servers", kafka.getBootstrapServers());
		props.put("group.id", groupId);
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(Collections.singletonList("bank"));

		boolean running = true;
		List<EventEnvelope<BankEvent, Tuple0, Tuple0>> envelopes = new ArrayList<>();
		while(running) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
			for (ConsumerRecord<String, String> record : records) {
				final long offset = record.offset();
				if (offset >= lastOffset) {
					running = false;
				}
				envelopes.add(parsEnvelope(record.value()));
			}
			consumer.commitSync();
		}
		consumer.close();
		return envelopes;
	}

	private static Optional<Long> getEndOffsetIfNotReached(String topic, String kafkaServers) {
		KafkaConsumer<?, ?> consumer = consumer(kafkaServers);
		PartitionInfo partitionInfo = consumer.partitionsFor("foo").get(0);
		TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
		consumer.assign(Collections.singletonList(topicPartition));

		long position = consumer.position(topicPartition);
		consumer.seekToEnd(Collections.singletonList(topicPartition));
		final long endOffset = consumer.position(topicPartition);

		Optional<Long> result = Optional.empty();
		if(endOffset > 0 && endOffset > position) {
			result = Optional.of(consumer.position(topicPartition) - 1);
		}

		consumer.close();
		return result;
	}

	private static KafkaConsumer<String, String> consumer(String kafkaServers) {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new KafkaConsumer<>(properties);
	}

	public EventEnvelope<BankEvent, Tuple0, Tuple0> parsEnvelope(String value) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode node = (ObjectNode) mapper.readTree(value);
			CompletableFuture<EventEnvelope<BankEvent, Tuple0, Tuple0>> future = new CompletableFuture<>();
			EventEnvelopeJson.deserialize(
					node,
					eventFormat,
					JacksonSimpleFormat.empty(),
					JacksonSimpleFormat.empty(),
					(event, err) -> {
						future.completeExceptionally(new RuntimeException(err.toString()));
					},
					future::complete
			);
			return future.get();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
}
