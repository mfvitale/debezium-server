/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.connector.jdbc.JdbcChangeEventSink;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.QueryBinderResolver;
import io.debezium.connector.jdbc.RecordWriter;
import io.debezium.connector.jdbc.UnnestRecordWriter;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectResolver;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerSink;

/**
 * Implementation of the consumer that delivers change events to a JDBC database
 * using the Debezium JDBC connector.
 *
 * This consumer wraps JdbcChangeEventSink and manages the complete lifecycle including:
 * - Hibernate SessionFactory creation and cleanup
 * - StatelessSession management
 * - Database dialect resolution
 * - Configuration transformation from debezium.sink.jdbc.* properties
 * - Conversion from ChangeEvent to SinkRecord
 *
 * The consumer is responsible for SessionFactory lifecycle because JdbcChangeEventSink
 * expects a session to be provided but does not manage the factory itself.
 *
 * @author Mario Fiore Vitale
 */
@Named("jdbc")
@Dependent
public class JdbcChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.jdbc.";

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();
    private final ChangeEventToSinkRecordConverter converter = new ChangeEventToSinkRecordConverter();

    // Lifecycle managed components
    private SessionFactory sessionFactory;
    private StatelessSession session;
    private JdbcChangeEventSink changeEventSink;
    private JdbcChangeConsumerConfig config;

    /**
     * Initializes the JDBC sink by:
     * 1. Loading and transforming configuration
     * 2. Creating Hibernate SessionFactory
     * 3. Opening a StatelessSession
     * 4. Resolving the database dialect
     * 5. Creating the RecordWriter
     * 6. Creating the JdbcChangeEventSink
     */
    @PostConstruct
    void connect() {
        LOGGER.info("Initializing JDBC sink");

        try {
            // Load configuration
            Config mpConfig = ConfigProvider.getConfig();
            io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));

            this.config = new JdbcChangeConsumerConfig(configuration);
            JdbcSinkConnectorConfig jdbcConfig = config.getJdbcConfig();

            // Validate configuration
            jdbcConfig.validate();

            // Get connection URL from Hibernate configuration
            org.hibernate.cfg.Configuration hibernateConfig = jdbcConfig.getHibernateConfiguration();
            String connectionUrl = hibernateConfig.getProperty(org.hibernate.cfg.AvailableSettings.JAKARTA_JDBC_URL);
            LOGGER.info("JDBC connection URL: {}", connectionUrl);

            // Create SessionFactory - THIS IS CRITICAL
            // JdbcChangeEventSink doesn't manage this, so we must
            LOGGER.info("Creating Hibernate SessionFactory");
            this.sessionFactory = jdbcConfig.getHibernateConfiguration().buildSessionFactory();

            // Open stateless session for the sink
            LOGGER.debug("Opening Hibernate StatelessSession");
            this.session = sessionFactory.openStatelessSession();

            // Resolve database dialect
            DatabaseDialect dialect = DatabaseDialectResolver.resolve(jdbcConfig, sessionFactory);
            LOGGER.info("Resolved database dialect: {}", dialect.getClass().getSimpleName());

            // Create query binder resolver
            QueryBinderResolver queryBinderResolver = new QueryBinderResolver();

            // Create RecordWriter (mirrors JdbcSinkConnectorTask logic)
            RecordWriter recordWriter = createRecordWriter(
                    session, queryBinderResolver, jdbcConfig, dialect);

            // Create connector context for OpenLineage
            ConnectorContext connectorContext = new ConnectorContext(
                    "debezium-server-jdbc",
                    "jdbc",
                    "0",
                    Module.version(),
                    UUID.randomUUID(),
                    new java.util.HashMap<>());

            // Create the actual sink
            this.changeEventSink = new JdbcChangeEventSink(
                    jdbcConfig, session, dialect, recordWriter, connectorContext);

            LOGGER.info("JDBC sink initialized successfully");
            LOGGER.info("Insert mode: {}", jdbcConfig.getInsertMode());
            LOGGER.info("Schema evolution: {}", jdbcConfig.getSchemaEvolutionMode());
            LOGGER.info("Batch size: {}", jdbcConfig.getBatchSize());

        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize JDBC sink", e);
            // Clean up any partially initialized resources
            close();
            throw new DebeziumException("Failed to initialize JDBC sink", e);
        }
    }

    /**
     * Creates the appropriate RecordWriter based on configuration and dialect.
     *
     * Note: We use UnnestRecordWriter for all cases because:
     * 1. It has a public constructor (DefaultRecordWriter has protected constructor)
     * 2. When UNNEST is not enabled, it automatically delegates to standard batching
     * 3. This mirrors the behavior of JdbcSinkConnectorTask
     */
    private RecordWriter createRecordWriter(
                                            StatelessSession session,
                                            QueryBinderResolver queryBinderResolver,
                                            JdbcSinkConnectorConfig config,
                                            DatabaseDialect dialect) {

        if (config.isPostgresUnnestInsertEnabled()) {
            LOGGER.info("Using UnnestRecordWriter for PostgreSQL UNNEST optimization");
        }
        else {
            LOGGER.info("Using UnnestRecordWriter (will use standard JDBC batching)");
        }

        return new UnnestRecordWriter(session, queryBinderResolver, config, dialect);
    }

    /**
     * Handles a batch of change events by:
     * 1. Converting each ChangeEvent to SinkRecord
     * 2. Delegating to JdbcChangeEventSink for processing
     * 3. Marking records as processed via the committer
     */
    @Override
    public void handleBatch(
                            List<ChangeEvent<Object, Object>> records,
                            DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        LOGGER.debug("Processing batch of {} records", records.size());

        try {
            // Convert ChangeEvents to SinkRecords
            Collection<SinkRecord> sinkRecords = records.stream()
                    .map(converter::convert)
                    .collect(Collectors.toList());

            // Delegate to JdbcChangeEventSink
            // This handles all the complex logic: buffering, schema evolution,
            // insert/update/delete, retries, etc.
            changeEventSink.execute(sinkRecords);

            // Mark all records as processed
            for (ChangeEvent<Object, Object> record : records) {
                committer.markProcessed(record);
            }

            committer.markBatchFinished();

            LOGGER.debug("Successfully processed batch of {} records", records.size());

        }
        catch (Exception e) {
            LOGGER.error("Failed to process batch of {} records", records.size(), e);
            throw new DebeziumException("Failed to process batch", e);
        }
    }

    /**
     * Closes all managed resources in reverse order of creation:
     * 1. JdbcChangeEventSink (closes session)
     * 2. SessionFactory
     *
     * This is CRITICAL because JdbcChangeEventSink doesn't manage SessionFactory.
     */
    @PreDestroy
    @Override
    public void close() {
        LOGGER.info("Closing JDBC sink");

        // Close sink first (this closes the session)
        if (changeEventSink != null) {
            try {
                LOGGER.debug("Closing JdbcChangeEventSink");
                changeEventSink.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error closing JdbcChangeEventSink", e);
            }
            finally {
                changeEventSink = null;
            }
        }

        // Session should be closed by sink, but ensure it
        if (session != null && session.isOpen()) {
            try {
                LOGGER.debug("Closing StatelessSession");
                session.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error closing session", e);
            }
            finally {
                session = null;
            }
        }

        // Close SessionFactory - THIS IS CRITICAL
        // Must be done last and only by us since JdbcChangeEventSink doesn't do it
        if (sessionFactory != null && !sessionFactory.isClosed()) {
            try {
                LOGGER.info("Closing Hibernate SessionFactory");
                sessionFactory.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error closing SessionFactory", e);
            }
            finally {
                sessionFactory = null;
            }
        }

        LOGGER.info("JDBC sink closed");
    }

    @Override
    public Field.Set getConfigFields() {
        // Return all JDBC configuration fields for documentation/introspection
        if (config != null) {
            return config.getAllConfigurationFields();
        }
        return JdbcSinkConnectorConfig.ALL_FIELDS;
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
