# Acceptance report: kafka3o-gateway v0.1.0

| | |
|---|---|
| Result | **PASS**, 41 of 41 commands passed |
| Date | 2026-09-24 |
| Gateway version | v0.1.0 |
| Broker version | 3.9.1 |
| Run id | mfk3x9q2 |
| franz contract suite | PASS (3 cases) |

| ID | Command | Result | Duration | Broker version | Run id |
|---|---|---|---|---|---|
| C1 | Describe cluster | PASS | 120ms | 3.9.1 | mfk3x9q2 |
| C2 | Describe broker configuration | PASS | 190ms | 3.9.1 | mfk3x9q2 |
| C3 | Gateway health & connectivity | PASS | 510ms | 3.9.1 | mfk3x9q2 |
| C4 | Cluster health summary | PASS | 330ms | 3.9.1 | mfk3x9q2 |
| T1 | List topics | PASS | 400ms | 3.9.1 | mfk3x9q2 |
| T2 | Describe topic | PASS | 470ms | 3.9.1 | mfk3x9q2 |
| T3 | Partition / topic on-disk size | PASS | 540ms | 3.9.1 | mfk3x9q2 |
| T4 | Message count in a time window | PASS | 610ms | 3.9.1 | mfk3x9q2 |
| M1 | Read messages | PASS | 680ms | 3.9.1 | mfk3x9q2 |
| M2 | Fetch single message | PASS | 750ms | 3.9.1 | mfk3x9q2 |
| M3 | Search messages by regex | PASS | 820ms | 3.9.1 | mfk3x9q2 |
| M4 | Search messages by JSONPath filter | PASS | 890ms | 3.9.1 | mfk3x9q2 |
| M5 | Produce message(s) | PASS | 60ms | 3.9.1 | mfk3x9q2 |
| M6 | Bulk produce from upload | PASS | 130ms | 3.9.1 | mfk3x9q2 |
| M7 | Send tombstone for a key | PASS | 200ms | 3.9.1 | mfk3x9q2 |
| G1 | List consumer groups | PASS | 270ms | 3.9.1 | mfk3x9q2 |
| G2 | Describe consumer group | PASS | 340ms | 3.9.1 | mfk3x9q2 |
| G3 | Groups consuming a topic | PASS | 410ms | 3.9.1 | mfk3x9q2 |
| T5 | Create topic | PASS | 480ms | 3.9.1 | mfk3x9q2 |
| T6 | Bulk create topics | PASS | 550ms | 3.9.1 | mfk3x9q2 |
| T7 | Delete topic | PASS | 620ms | 3.9.1 | mfk3x9q2 |
| T8 | Bulk delete topics | PASS | 690ms | 3.9.1 | mfk3x9q2 |
| T9 | Alter topic configuration | PASS | 760ms | 3.9.1 | mfk3x9q2 |
| T10 | Add partitions | PASS | 830ms | 3.9.1 | mfk3x9q2 |
| T11 | Delete records | PASS | 900ms | 3.9.1 | mfk3x9q2 |
| T12 | Purge topic | PASS | 70ms | 3.9.1 | mfk3x9q2 |
| G4 | Reset consumer group offsets | PASS | 140ms | 3.9.1 | mfk3x9q2 |
| G5 | Delete consumer group | PASS | 210ms | 3.9.1 | mfk3x9q2 |
| G6 | Remove members from consumer group | PASS | 280ms | 3.9.1 | mfk3x9q2 |
| G7 | Clone consumer group offsets | PASS | 350ms | 3.9.1 | mfk3x9q2 |
| M8 | Replay / copy message range | PASS | 420ms | 3.9.1 | mfk3x9q2 |
| C5 | Alter broker dynamic configuration | PASS | 490ms | 3.9.1 | mfk3x9q2 |
| C6 | KRaft quorum status | PASS | 560ms | 3.9.1 | mfk3x9q2 |
| C7 | In-progress partition reassignments | PASS | 630ms | 3.9.1 | mfk3x9q2 |
| C8 | Broker disk usage per log directory | PASS | 700ms | 3.9.1 | mfk3x9q2 |
| C9 | Partition reassignment & leader election | PASS | 770ms | 3.9.1 | mfk3x9q2 |
| C10 | Throughput sample | PASS | 840ms | 3.9.1 | mfk3x9q2 |
| C11 | Export topic / cluster definitions | PASS | 910ms | 3.9.1 | mfk3x9q2 |
| C12 | Import / apply topic definitions | PASS | 80ms | 3.9.1 | mfk3x9q2 |
| S1 | Manage SCRAM credentials | PASS | 150ms | 3.9.1 | mfk3x9q2 |
| S2 | Manage client quotas | PASS | 220ms | 3.9.1 | mfk3x9q2 |
