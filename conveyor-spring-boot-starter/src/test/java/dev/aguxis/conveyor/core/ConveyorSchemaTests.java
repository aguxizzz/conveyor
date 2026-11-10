package dev.aguxis.conveyor.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import dev.aguxis.conveyor.TestcontainersConfiguration;

/**
 * Adopting conveyor into an existing app database: its migrations must run next to the app's tables
 * and the app's own Flyway history, without touching either.
 */
@SpringBootTest(properties = { "conveyor.worker.enabled=false", "conveyor.scheduler.enabled=false",
		"conveyor.retention.enabled=false" })
@Import(TestcontainersConfiguration.class)
class ConveyorSchemaTests {

	@Autowired
	JdbcClient jdbc;

	@Autowired
	JdbcConnectionDetails db;

	@Test
	void migratesADatabaseThatAlreadyHasTheAppsTables() {
		// a database of its own, since the shared one was already migrated when the context started
		jdbc.sql("DROP DATABASE IF EXISTS existing_app").update();
		jdbc.sql("CREATE DATABASE existing_app").update();
		var dataSource = new DriverManagerDataSource(db.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/existing_app$1"),
				db.getUsername(), db.getPassword());
		var app = JdbcClient.create(dataSource);
		app.sql("CREATE TABLE orders (id bigserial PRIMARY KEY)").update();
		app.sql("CREATE TABLE flyway_schema_history (installed_rank int PRIMARY KEY)").update();

		ConveyorSchema.migrate(dataSource, "conveyor_schema_history");
		ConveyorSchema.migrate(dataSource, "conveyor_schema_history"); // a second start finds nothing to do

		assertThat(app.sql("SELECT count(*) FROM jobs").query(Long.class).single()).isZero();
		assertThat(app.sql("SELECT count(*) FROM orders").query(Long.class).single()).isZero();
		// the app's history is left alone; conveyor's has the baseline plus every migration, applied once
		assertThat(app.sql("SELECT count(*) FROM flyway_schema_history").query(Long.class).single()).isZero();
		assertThat(app.sql("SELECT version FROM conveyor_schema_history WHERE success ORDER BY installed_rank")
				.query(String.class).list()).containsExactly("0", "1", "2", "3", "4", "5", "6");
	}
}
