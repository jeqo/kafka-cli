package kafka.cli.producer.datagen;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.SchemaParseException;
import org.apache.kafka.common.config.ConfigException;

public enum Quickstart {
  CLICKSTREAM_CODES("clickstream_codes_schema.avro", "code"),
  CLICKSTREAM("clickstream_schema.avro", "ip"),
  CLICKSTREAM_USERS("clickstream_users_schema.avro", "user_id"),
  ORDERS("orders_schema.avro", "orderid"),
  RATINGS("ratings_schema.avro", "rating_id"),
  USERS("users_schema.avro", "userid"),
  USERS_("users_array_map_schema.avro", "userid"),
  PAGEVIEWS("pageviews_schema.avro", "viewtime"),
  STOCK_TRADES("stock_trades_schema.avro", "symbol"),
  INVENTORY("inventory.avro", "id"),
  PRODUCT("product.avro", "id"),
  PURCHASES("purchase.avro", "id"),
  TRANSACTIONS("transactions.avro", "transaction_id"),
  STORES("stores.avro", "store_id"),
  CREDIT_CARDS("credit_cards.avro", "card_id");

  static final Set<String> configValues = new HashSet<>();

  static {
    for (Quickstart q : Quickstart.values()) {
      configValues.add(q.name().toLowerCase());
    }
  }

  private final String schemaFilename;
  private final String keyName;

  Quickstart(String schemaFilename, String keyName) {
    this.schemaFilename = schemaFilename;
    this.keyName = keyName;
  }

  public InputStream getSchemaFilename() {
    try {
      return Quickstart.class.getClassLoader().getResourceAsStream(schemaFilename);
    } catch (SchemaParseException i) {
      // log.error("Unable to parse the provided schema", i);
      throw new ConfigException("Unable to parse the provided schema");
    }
  }

  public String getSchemaKeyField() {
    return keyName;
  }
}
