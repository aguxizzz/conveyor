package dev.aguxis.conveyor.autoconfigure;

import javax.sql.DataSource;

import org.springframework.beans.factory.InitializingBean;

import dev.aguxis.conveyor.core.ConveyorSchema;

/**
 * Runs conveyor's migrations while the context starts. The repositories depend on this bean, so the
 * tables exist before anything can enqueue, even an app's own startup code.
 */
class ConveyorSchemaInitializer implements InitializingBean {

	private final DataSource dataSource;
	private final ConveyorProperties.Schema settings;

	ConveyorSchemaInitializer(DataSource dataSource, ConveyorProperties.Schema settings) {
		this.dataSource = dataSource;
		this.settings = settings;
	}

	@Override
	public void afterPropertiesSet() {
		if (settings.migrate()) {
			ConveyorSchema.migrate(dataSource, settings.historyTable());
		}
	}
}
