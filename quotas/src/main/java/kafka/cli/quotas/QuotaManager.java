package kafka.cli.quotas;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import kafka.cli.quotas.Quotas.Quota;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

public class QuotaManager {

  final AdminClient kafkaAdmin;

  public QuotaManager(AdminClient adminClient) {
    this.kafkaAdmin = adminClient;
  }

  public Quotas all() {
    final var filter = ClientQuotaFilter.all();
    return query(filter);
  }

  private Quotas query(ClientQuotaFilter filter) {
    try {
      final var all = kafkaAdmin.describeClientQuotas(filter).entities().get();
      final var quotas = new ArrayList<Quota>(all.size());
      for (final var entity : all.keySet()) {
        final var constraints = all.get(entity);
        final var quota = Quota.from(entity, constraints);
        quotas.add(quota);
      }
      return new Quotas(quotas);
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public Quotas allByUsers() {
    final var conditions = List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));
    final var filter = ClientQuotaFilter.contains(conditions);
    return query(filter);
  }

  public Quotas allByClients() {
    final var conditions =
        List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID));
    final var filter = ClientQuotaFilter.contains(conditions);
    return query(filter);
  }

  public Quotas allByIps() {
    final var conditions = List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.IP));
    final var filter = ClientQuotaFilter.contains(conditions);
    return query(filter);
  }

  public Quotas byUsers(
      Map<String, List<String>> users, boolean includeDefault, boolean onlyMatch) {
    final var perEntity = Quotas.empty();
    final var defaults = Quotas.empty();
    for (final var user : users.keySet()) {
      if (includeDefault || !onlyMatch) {
        defaults.append(fromUserClientDefault(user));
      }
      for (final var client : users.get(user)) {
        final var quotas = onlyByUserClient(user, client);
        perEntity.append(quotas);
      }
    }
    if (onlyMatch) {
      if (includeDefault) {
        return perEntity.append(defaults);
      }
      return perEntity;
    } else {
      return perEntity.append(defaults);
    }
  }

  public Quotas byUsers(List<String> users, boolean userDefault, boolean onlyMatch) {
    return by(ClientQuotaEntity.USER, users, userDefault, onlyMatch);
  }

  public Quotas byClients(List<String> clientIds, boolean clientIdDefault, boolean onlyMatch) {
    return by(ClientQuotaEntity.CLIENT_ID, clientIds, clientIdDefault, onlyMatch);
  }

  public Quotas byIps(List<String> ips, boolean ipDefault, boolean onlyMatch) {
    return by(ClientQuotaEntity.IP, ips, ipDefault, onlyMatch);
  }

  public Quotas by(
      String entityType, List<String> entities, boolean includeDefault, boolean onlyMatch) {
    final var perEntity =
        entities.stream().map(e -> onlyBy(entityType, e)).reduce(Quotas.empty(), Quotas::append);
    if (onlyMatch) {
      if (includeDefault) {
        return perEntity.append(fromDefault(entityType));
      }
      return perEntity;
    } else {
      return perEntity.append(fromDefault(entityType));
    }
  }

  Quotas fromDefault(String entityType) {
    final var components = ClientQuotaFilterComponent.ofDefaultEntity(entityType);
    final var filter = ClientQuotaFilter.containsOnly(List.of(components));
    return query(filter);
  }

  Quotas fromUserClientDefault(String user) {
    final var byUser = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, user);
    final var byClient = ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.CLIENT_ID);
    final var filter = ClientQuotaFilter.containsOnly(List.of(byClient));
    return query(filter);
  }

  Quotas onlyBy(String entityType, String entity) {
    final var components = ClientQuotaFilterComponent.ofEntity(entityType, entity);
    final var filter = ClientQuotaFilter.containsOnly(List.of(components));
    return query(filter);
  }

  Quotas onlyByUserClient(String user, String client) {
    final var byUser = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, user);
    final var byClient = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID, client);
    final var filter = ClientQuotaFilter.containsOnly(List.of(byUser, byClient));
    return query(filter);
  }

  public void create(Quota quota) throws ExecutionException, InterruptedException {
    kafkaAdmin.alterClientQuotas(List.of(quota.toAlteration())).all().get();
  }

  public void delete(Quota quota) throws ExecutionException, InterruptedException {
    kafkaAdmin.alterClientQuotas(List.of(quota.toAlteration())).all().get();
  }

  public void delete(Quotas quotas) throws ExecutionException, InterruptedException {
    final var alterations = quotas.toDeleteAlterations();
    kafkaAdmin.alterClientQuotas(alterations).all().get();
  }
}
