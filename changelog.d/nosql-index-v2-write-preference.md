### New Features

- NoSQL persistence supports V2 index reads and opt-in V2 writes through
  `polaris.persistence.preferred-index-write-version` (`1` by default).

### Upgrade notes

- Keep the NoSQL index write preference at `1` until all active nodes and intended
  rollback binaries can read V2. Setting it to `2` allows new and rewritten indexes
  to use V2. Lowering it later does not demote existing V2 data or restore V1-only
  binary compatibility.
