package dev.aguxis.conveyor.core;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.OptionalLong;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Leader election on a row of {@code leader_leases}: whoever holds an unexpired lease leads.
 * <p>
 * The leader renews its lease well before it expires; if it dies, the lease runs out and the next
 * candidate to ask takes over. Like job leases, expiry alone can't stop a leader that paused (GC, a
 * slow network) and doesn't know it was replaced, so every change of holder increments {@code term},
 * a fencing token. Work done as leader goes through {@link #lockIfHeld}, which only succeeds for the
 * current term.
 */
@Repository
public class LeaderLease {

	public record Holder(String holder, long term, Instant expiresAt) {
	}

	private final JdbcClient jdbc;

	public LeaderLease(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	/**
	 * Acquires the lease, or renews it if {@code candidate} already holds it, for {@code ttl} from now.
	 * One statement, so two candidates racing for an expired lease can't both win: the row lock taken
	 * by {@code ON CONFLICT DO UPDATE} makes the second one re-check against the first one's result.
	 *
	 * @return the term led by {@code candidate}, or empty if someone else holds an unexpired lease
	 */
	public OptionalLong tryAcquire(String name, String candidate, Duration ttl) {
		return jdbc.sql("""
				INSERT INTO leader_leases AS l (name, holder, term, expires_at)
				VALUES (:name, :candidate, 1, now() + make_interval(secs => :ttlSeconds))
				ON CONFLICT (name) DO UPDATE
				   SET holder = EXCLUDED.holder,
				       term = CASE WHEN l.holder = EXCLUDED.holder THEN l.term ELSE l.term + 1 END,
				       expires_at = EXCLUDED.expires_at
				 WHERE l.holder = EXCLUDED.holder OR l.expires_at <= now()
				RETURNING term
				""")
				.param("name", name)
				.param("candidate", candidate)
				.param("ttlSeconds", ttl.toMillis() / 1000.0)
				.query(Long.class)
				.optional()
				.map(OptionalLong::of)
				.orElse(OptionalLong.empty());
	}

	/**
	 * Gives up the lease right away (on shutdown), so a follower can take over without waiting for it
	 * to expire. The row stays: its term must keep counting up.
	 */
	public void release(String name, String holder) {
		jdbc.sql("UPDATE leader_leases SET expires_at = now() WHERE name = :name AND holder = :holder")
				.param("name", name)
				.param("holder", holder)
				.update();
	}

	/**
	 * Checks that {@code holder} still leads in {@code term} and locks the lease row in share mode until
	 * the current transaction ends. Must run inside a transaction.
	 * <p>
	 * The lock is what makes this a fence and not just a check: taking over needs to update the row, so
	 * a new leader waits until this transaction commits. Without it, the lease could expire between the
	 * check and the commit and both leaders would act at once.
	 */
	boolean lockIfHeld(String name, String holder, long term) {
		return jdbc.sql("""
				SELECT 1 FROM leader_leases
				 WHERE name = :name AND holder = :holder AND term = :term AND expires_at > now()
				   FOR SHARE
				""")
				.param("name", name)
				.param("holder", holder)
				.param("term", term)
				.query(Integer.class)
				.optional()
				.isPresent();
	}

	public Optional<Holder> current(String name) {
		return jdbc.sql("SELECT holder, term, expires_at FROM leader_leases WHERE name = :name AND expires_at > now()")
				.param("name", name)
				.query((rs, n) -> new Holder(rs.getString("holder"), rs.getLong("term"),
						rs.getObject("expires_at", OffsetDateTime.class).toInstant()))
				.optional();
	}
}
