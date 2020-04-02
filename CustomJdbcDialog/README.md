# Custom JDBC dialogs for Kafka Connect

This project contains custom a list of interesting custom JDBC dialog
for the Kafka Connect JDBC connector.

## Use

If you wanna use this project,

 - compile: _mvn clean package_
 - deploy it to your Kafka Connect cluster path, for example into
   /usr/share/java or /usr/share/java/kafka-connect-jdbc
 - then please restart the connect workers, and the cluster will pick up
   the new Dialog.

When the cluster pick the new dialog you will see in the logs, something
like:

```
[2020-04-02 16:16:42,325] DEBUG Found 'CustomOracleJdbcDialog' provider
class com.purbon.kafka.connect.jdbc.CustomOracleJdbcDialog$Provider
(io.confluent.connect.jdbc.dialect.DatabaseDialects)
```
if the cluster LOGS are running in debug mode.
