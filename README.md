## Nifi Custom Log4j Logger Processor

### Description

A NiFi processor that logs FlowFile content/attribute using Log4j with customizable log levels and prefixes in a separate log file based on Log4j xml configuration.

### Properties

- `log4jConfiguration`: Paste your Log4j XML configuration here.
- `type`: Specifies the logging type for incoming data. Default is 'error'.
- `prefix`: Specify the prefix for log messages.
- `source`: Specifies the source of data to log. ('flowfile-content', 'flowfile-attribute', 'flowfile-content + flowfile-attribute')
- `attributes`: Comma-separated attributes to be added to the log statement. Use with 'flowfile-content' logging source only.

### Build

`mvn clean install`

### Setup

Add the `nifi-custom-log4j-logger-nar/target/nifi-custom-log4j-logger-nar-1.0.nar` file in the `lib/` directory.

