package dev.aguxis.conveyor.core;

import java.util.Map;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;

/**
 * Creates and upgrades conveyor's tables ({@code jobs}, {@code schedules}, {@code leader_leases}) in the
 * app's database, with migrations that ship inside this jar.
 * <p>
 * They're tracked in a history table of their own, so they never mix with the app's own Flyway
 * migrations: both can have a V1, and each side upgrades on its own schedule.
 */
public final class ConveyorSchema {

	private ConveyorSchema() {
	}

	/** Applies any pending migration. Safe to call on every start and from several instances at once. */
	public static void migrate(DataSource dataSource, String historyTable) {
		Flyway.configure()
				.dataSource(dataSource)
				.locations("classpath:db/conveyor")
				.table(historyTable)
				// The app's database usually has tables already. Without a baseline Flyway refuses to touch
				// a non-empty schema; baselining at 0 marks it as adopted and still runs V1 onwards.
				.baselineOnMigrate(true)
				.baselineVersion("0")
				// Flyway's default lock is held in an open transaction, which CREATE INDEX CONCURRENTLY (V5)
				// would wait on forever. The session-level lock still keeps two instances from migrating at once.
				.configuration(Map.of("flyway.postgresql.transactional.lock", "false"))
				.load()
				.migrate();
	}
}
