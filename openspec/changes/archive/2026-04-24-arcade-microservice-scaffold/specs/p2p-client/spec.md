## ADDED Requirements

### Requirement: Listen for new blocks
The P2P client SHALL connect to the Bitcoin SV network using the Teranode libp2p client and listen for new block announcements.

#### Scenario: New block announced
- **WHEN** a new block is announced on the P2P network
- **THEN** the client SHALL publish a message to the `block` Kafka topic containing the block hash and block header metadata

#### Scenario: P2P connection lost
- **WHEN** the connection to the P2P network is lost
- **THEN** the client SHALL attempt to reconnect with exponential backoff and log the disconnection event

### Requirement: Block notification message format
The P2P client SHALL publish block notifications to Kafka in a structured format containing the block hash, block height, previous block hash, and timestamp.

#### Scenario: Publish block notification
- **WHEN** a block with hash `abc123` at height 800000 is received
- **THEN** the client SHALL publish a Kafka message to the `block` topic with block hash, height, previous hash, and the timestamp of receipt

### Requirement: Duplicate block handling
The P2P client SHALL handle duplicate block announcements gracefully without publishing duplicate Kafka messages.

#### Scenario: Same block announced twice
- **WHEN** the same block hash is announced twice within a short time window
- **THEN** the client SHALL publish only one Kafka message for that block
