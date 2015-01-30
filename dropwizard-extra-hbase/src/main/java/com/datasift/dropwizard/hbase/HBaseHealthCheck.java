package com.datasift.dropwizard.hbase;

import com.codahale.metrics.health.HealthCheck;
import com.stumbleupon.async.TimeoutException;
import org.hbase.async.TableNotFoundException;

/**
 * A {@link HealthCheck} for an HBase table using an {@link HBaseClient}.
 */
public class HBaseHealthCheck extends HealthCheck {

    private final HBaseClient client;
    private final String[] tables;

    /**
     * Checks the health of the given {@link HBaseClient} by connecting and testing for the given
     * {@code table}.
     *
     * @param client the client to check the health of.
     * @param tables the name of the tables to look for. Okay as long as at least one table is present.
     */
    public HBaseHealthCheck(final HBaseClient client, final String... tables) {
        this.client = client;
        this.tables = tables;
    }

    /**
     * Checks the health of the configured {@link HBaseClient} by using it to test for the
     * configured {@code table}.
     *
     * @return {@link Result#healthy()} if the client can be used to confirm if any of the table exists; or
     *         {@link Result#unhealthy(String)} either if the table does not exist or the client
     *         times out while checking for the table.
     *
     * @throws Exception if an unexpected Exception occurs while checking the health of the client.
     */
    @Override
    protected Result check() throws Exception {
        boolean isHealthy = false;
        StringBuilder errorMsg = new StringBuilder();
        for (String tableName : tables) {
            try {
                client.ensureTableExists(tableName.getBytes()).joinUninterruptibly(5000);
                isHealthy = true;
                break;
            } catch (final TimeoutException e) {
                errorMsg.append(String.format("Timed out checking for '%s' after 5 seconds. ", tableName));
                isHealthy = false;
            } catch (final TableNotFoundException e) {
                isHealthy = false;
                errorMsg.append(String.format("Table '%s' does not exist. ", tableName));
            }
        }

        if (isHealthy) {
            return Result.healthy();
        } else {
            return Result.unhealthy(errorMsg.toString());
        }
    }
}
