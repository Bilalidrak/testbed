bashcat > /root/FreeSwitch_Implement_scripts/README_testbed.md << 'EOF'
# FreeSWITCH Testbed

A load testing and disposition validation framework for FreeSWITCH-based call systems.

---

## What It Does

Fires real FreeSWITCH calls with specific audio files and validates the AI disposition result against MongoDB.
Portal UI / CLI
↓
RabbitMQ (test_bed_queue)
↓
Consumer Handler
↓
fl.py / testbed-bilal.py
↓
FreeSWITCH → SBC → AI/AMD System
↓
MongoDB (result stored)
↓
MATCH / MISMATCH Report

---

## Scripts

### `testbed-bilal.py` — CLI Version
Run directly from terminal. Full-featured with MongoDB validation, disposition pool, batch control.

**How to run:**
```bash
python3 testbed-bilal.py \
  --sbc srv-dev.idrakai.com \
  --extension 811 \
  --concurrent 10 \
  --duration 120 \
  --campaign FE
```

**Interactive menu will ask:**
- Which disposition folders to test
- Batch size
- Long call percentage

---

### `testbed-sumair.py` — Sumair's Version
Alternate CLI version. Same concept, different implementation.

---

## Environment Setup

Create a `.env` file with:
MONGO_URI="mongodb://username:password@host:port/dbname?authSource=admin"

Install dependencies:
```bash
pip install pymongo
```

---

## Campaigns & Audio Paths

| Campaign | Audio Path |
|----------|-----------|
| FE | `/usr/share/freeswitch/sounds/happy_test_fe/` |
| Medicare | `/usr/share/freeswitch/sounds/happy_test_medicare/` |
| ACA | `/usr/share/freeswitch/sounds/happy_test_aca/` |
| Solar | `/usr/share/freeswitch/sounds/happy_test_solar/` |
| MVA | `/usr/share/freeswitch/sounds/happy_test_acident/` |
| Home Improvement | `/usr/share/freeswitch/sounds/happy_test_hi/` |
| Home Warranty | `/usr/share/freeswitch/sounds/happy_test_hw/` |

---

## FE Campaign Audio Folders

| Folder | Disposition | Files |
|--------|-------------|-------|
| AM | Answering Machine | 50 |
| DNQ | Do Not Qualify | 50 |
| Ringing | Ringing/RI | 20 |
| Smart_Am | Smart Answering Machine | 52 |
| XFER | Transfer | 50 |
| all | All mixed | 222 |

---

## DB Validation

Each call is validated against MongoDB after completion:
✅ MATCH    — AI detected correct disposition
❌ MISMATCH — AI detected different disposition
⚠️ NOT_FOUND — Call not found in DB yet

**Key DB fields:**
status      — AI final result (A, DNQ, NI, XFER, DNC etc)
isProcessed — 1 = processed by AI
leadId      — matches X-VICIdial-Lead-Id header
callerId    — matches X-VICIdial-Caller-Id header

---

## Infrastructure

| Component | Value |
|-----------|-------|
| Server | freeswitch-v5line (157.180.116.5) |
| RabbitMQ | 77.42.22.28:30672 |
| Queue | test_bed_queue |
| Dev MongoDB | 77.42.22.28:31023 |
| Dev SBC | srv-dev.idrakai.com |
| PM2 Process | test-bed-consumer |

---

## Portal Integration (Task #5659)

The `fl.py` script is integrated with the v6 portal via RabbitMQ consumer.

**Portal message format:**
```json
{
  "kamilioDNS": "srv-dev.idrakai.com",
  "concurrentCallCount": 10,
  "callDurationInSec": 120,
  "extensions": 811,
  "clientId": "TestDEV",
  "availableDispo": "AM",
  "batchSize": 2,
  "longCallPercentage": 1,
  "campaign": "FE"
}
```

**Consumer start:**
```bash
cd ~/V5-Lines-Scripts
source venv/bin/activate
pm2 start "python3 -m consumer.test_bed_consumer" --name test-bed-consumer
```

---

## Developed By
Bilal Waheed — Junior DevOps Engineer, IDRAK AI (Botmatic Team)
EOF
Ab GitHub pe push karo directly from local:
bash# Local machine pe
cd ~/path/to/testbed/repo
cp /path/to/README_testbed.md README.md
git add README.md
git commit -m "Add comprehensive README"
git push
🎯
