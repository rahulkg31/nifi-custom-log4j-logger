package org.apache.nifi.processors.log4j;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({ "log4j", "logger", "logging" })
@CapabilityDescription("A NiFi processor that logs FlowFile content using Log4j with customizable log levels and prefixes.")
public class Log4jLogger extends AbstractProcessor {
	public static final PropertyDescriptor LOG4J_CONFIGURATION = new PropertyDescriptor.Builder()
			.name("log4jConfiguration")
			.description("Paste your Log4j XML configuration here").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder().name("type")
			.description("Specifies the logging type for incoming data. Default is 'error'.").required(true)
			.allowableValues("info", "error").defaultValue("error").build();

	public static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder().name("prefix").description("Specify the prefix for log messages.")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor SOURCE = new PropertyDescriptor.Builder().name("source")
			.description("Specifies the source of data to log.").required(true)
			.allowableValues("flowfile-content", "flowfile-attribute", "flowfile-content+flowfile-attribute")
			.defaultValue("flowfile-content").build();

	public static final PropertyDescriptor ATTRIBUTES = new PropertyDescriptor.Builder()
			.name("attributes")
			.description(
					"Comma-separated attributes to be added to the log statement. Use with 'flowfile-content' logging source only.")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").description("Success")
			.build();
	public static final Relationship FAILURE = new Relationship.Builder().name("FAILURE")
			.description("Failed to process").build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	private String prefix;
	private String type;
	private String source;
	private String[] attributes;
	private Logger logger;
	private String flowfileAttributes;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(LOG4J_CONFIGURATION);
		descriptors.add(TYPE);
		descriptors.add(PREFIX);
		descriptors.add(SOURCE);
		descriptors.add(ATTRIBUTES);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		relationships.add(FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) throws IOException {
		// prefix
		prefix = context.getProperty(PREFIX).getValue();
		prefix = (prefix == null || "".equals(prefix)) ? "" : "[" + prefix + "]";

		// type
		type = context.getProperty(TYPE).getValue();

		// data
		source = context.getProperty(SOURCE).getValue();
		String attributeStr = context.getProperty(ATTRIBUTES).getValue();
		attributes = (attributeStr != null)
				? context.getProperty(ATTRIBUTES).getValue().split(",")
				: null;

		// configuration
		String log4jConfiguration = context.getProperty(LOG4J_CONFIGURATION).getValue();
		ByteArrayInputStream inputStream = new ByteArrayInputStream(
				log4jConfiguration.getBytes(StandardCharsets.UTF_8));
		LoggerContext loggerContext = Configurator.initialize(null, new ConfigurationSource(inputStream));
		logger = (Logger) loggerContext.getLogger(Log4jLogger.class);
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		// attribute string
		flowfileAttributes = "";
		if ("flowfile-attribute".equals(source) || "flowfile-content+flowfile-attribute".equals(source)) {
			flowfileAttributes = "flowfile-attribute: " + flowFile.getAttributes().toString();
		} else if (attributes != null) {
			Map<String, String> map = flowFile.getAttributes();
			Map<String, String> newMap = new HashMap<>();
			for (int i = 0; i < attributes.length; i++) {
				String attribute = attributes[i];
				newMap.put(attribute, map.get(attribute));
			}
			flowfileAttributes = "flowfile-attribute: " + newMap.toString();
		}

		try {
			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(InputStream rawIn) throws IOException {
					try (final InputStream in = new BufferedInputStream(rawIn)) {
						String str = prefix + " ";
						if ("flowfile-content".equals(source)) {
							str = str + "flowfile-content: " + IOUtils.toString(in, "UTF-8");
						}
						str = str + " " + flowfileAttributes;
						if ("error".equals(type)) {
							logger.error(str);
						} else {
							logger.info(str);
						}
					}
				}
			});
		} catch (final ProcessException pe) {
			getLogger().error("Failed to process {} data due to {}; transferring to failure",
					new Object[] { flowFile, pe });
			session.transfer(flowFile, FAILURE);
			return;
		}

		session.transfer(flowFile, SUCCESS);
	}
}
