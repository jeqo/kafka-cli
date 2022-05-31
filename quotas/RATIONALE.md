# Rationale

## Requirements

- [x] List all quotas
  - [ ] quotas tree
- [x] List effective quotas an application, by client-id, user, or both.
- [x] Add quotas
  - [ ] (with dry-run)
- [x] Remove quotas 
  - [ ] (with dry-run)
- [ ] Envision metrics applications: depending on the existing quotas, which metrics are created?

### Sample scenarios

- Decide a quota entity:
  - (client-id?)
  - (or)
  - ((user-id?)
    - (and?)
  - (user-id + client-id?))
  
#### S1: Only Client ID

- Add a default quota per client id.
- Add a couple of custom quotas
- Remove a quota

- List
- Estimate
- Query which quota applies to a client-id

#### S2: Only User ID

- Add a default quota per user id.
- Add a couple of custom quotas
- Remove a quota

- List
- Estimate
- Query which quota applies to a user-id

#### S3: User ID and Client ID

- (extending previous)
- List
- Estimate
- Query which quota applies to a user+client-id

#### S4: All

- (extend previous with scenario 1)

### Research

#### Other CLIs

- https://docs.lenses.io/5.0/tools/cli/kafka/quotas/
- https://julieops.readthedocs.io/en/3.x/futures/use-of-quotas.html?

#### Quotas docs in Kafka platforms

- https://docs.confluent.io/platform/current/kafka/post-deployment.html#enforcing-client-quotas
- https://ibm.github.io/event-streams/administering/quotas/
- https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/kafka_admin_quotas.html

## Decisions


