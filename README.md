# golang-real-time-data-processing-pipeline
real-time data processing pipline implemented in golang and using rabbitmq

**message flow diagram**
```
PRODUCER
│
├──valid message──→ logs.ingest ──→ logs.to-validate
│                                      │
│                                  VALIDATOR
│                                      ├──PASS──→ logs.data 
│                                      │          (routing: logs.valid)
│                                      │              ↓
│                                      │          TRANSFORMER
│                                      │              │
│                                      │              └──→ logs.data
│                                      │                   (routing: logs.processed)
│                                      │
│                                      └──FAIL (Nack)──→ logs.dead-letter-x
│                                                         (routing: logs.invalid)
│                                                             ↓
└──invalid message────────────────────────────────────────→ logs.dead-letter-q
                                                              ↓
                                                          INSPECTOR
```
