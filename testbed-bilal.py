#!/usr/bin/env python3
"""
FreeSWITCH Testbed — Deterministic Disposition Routing & Validation Framework
Implements: t.pdf requirements
"""
import time, random, csv, threading, subprocess, signal, sys, re, os, glob, stat, argparse, resource
from datetime import datetime
from collections import defaultdict
try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False
# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION — edit before running
# ═══════════════════════════════════════════════════════════════════════════
MONGO_URI = os.environ.get("MONGO_URI", "")
MONGO_DB         = "idrakdb"
MONGO_COLLECTION = "Calls"
DEFAULT_SBC    = "sbcserver12.idrakai.com"
DEFAULT_EXT    = "911"
DEFAULT_AUDIO  = "/usr/share/freeswitch/sounds/happy_test/"
DEFAULT_CLIENT = "roundrobin"
CAMPAIGN_PATHS = {
    # Short aliases (CLI / legacy)
    "fe":               "/usr/share/freeswitch/sounds/happy_test_fe/",
    "solar":            "/usr/share/freeswitch/sounds/happy_test_solar/",
    "hi":               "/usr/share/freeswitch/sounds/happy_test_hi/",
    "hw":               "/usr/share/freeswitch/sounds/happy_test_hw/",
    "accident":         "/usr/share/freeswitch/sounds/happy_test_acident/",
    # Portal ENUM_CAMPAIGN_TYPES (case-insensitive via .lower() lookup)
    "medicare":         "/usr/share/freeswitch/sounds/happy_test_medicare/",
    "aca":              "/usr/share/freeswitch/sounds/happy_test_aca/",
    "home improvement": "/usr/share/freeswitch/sounds/happy_test_hi/",
    "home warranty":    "/usr/share/freeswitch/sounds/happy_test_hw/",
    "medalert":         "/usr/share/freeswitch/sounds/happy_test_medalert/",
    "mva":              "/usr/share/freeswitch/sounds/happy_test_acident/",
}
VALIDATION_DELAY   = 15   # seconds to wait before first DB lookup
VALIDATION_RETRIES = 3    # max DB lookup retries per call
POOL_SIZE          = 10000  # pre-built disposition pool size
TMP_FILE_MAX_AGE_HOURS = 48  # delete /tmp testbed CSV files older than this
# Folders to exclude from disposition testing (combined/utility folders)
EXCLUDED_FOLDERS   = {"all", "vicidial"}
# Per-disposition audio override for endless_playback
# Folder files are still used for Header-Soundfile ID
DISPO_PLAYBACK_OVERRIDE = {}
# Real US area codes for realistic caller-ID generation
AREA_CODES = [
    '201','202','203','205','206','207','208','209','210','212','213','214','215','216','217','218','219',
    '224','225','228','229','231','234','239','240','248','251','252','253','254','256','260','262','267',
    '269','270','276','281','301','302','303','304','305','307','308','309','310','312','313','314','315',
    '316','317','318','319','320','321','323','325','330','331','334','336','337','339','346','347','351',
    '352','360','361','364','369','380','385','386','401','402','404','405','406','407','408','409','410',
    '412','413','414','415','417','419','423','424','425','430','432','434','435','440','442','443','447',
    '458','463','469','470','475','478','479','480','484','501','502','503','504','505','507','508','509',
    '510','512','513','515','516','517','518','520','530','531','534','539','540','541','551','559','561',
    '562','563','564','567','570','571','573','574','575','580','585','586','601','602','603','605','606',
    '607','608','609','610','612','614','615','616','617','618','619','620','623','626','628','629','630',
    '631','636','641','646','650','651','657','660','661','662','667','669','678','681','682','701','702',
    '703','704','706','707','708','712','713','714','715','716','717','718','719','720','724','725','727',
    '731','732','734','737','740','743','747','754','757','760','762','763','765','769','770','771','772',
    '773','774','775','779','781','785','786','801','802','803','804','805','806','808','810','812','813',
    '814','815','816','817','818','828','830','831','832','843','845','847','848','850','854','856','857',
    '858','859','860','862','863','864','865','870','872','878','901','903','904','906','907','908','909',
    '910','912','913','914','915','916','917','918','919','920','925','928','929','931','936','937','938',
    '940','941','947','949','951','952','954','956','959','970','971','972','973','975','978','979','980',
    '984','985','989',
]
# ═══════════════════════════════════════════════════════════════════════════
#  Disposition normalization
# ═══════════════════════════════════════════════════════════════════════════
DISPO_ALIASES = {
    "RAXFER": "XFER", "XFER": "XFER", "TRANSFER": "XFER",
    "A": "AM",  "AM": "AM",  "AMD": "AM", "MACHINE": "AM",
    "SA": "Smart_Am", "SMART_AM": "Smart_Am",
    "DNQ": "DNQ", "DNC": "DNC", "D": "DNC",
    "NI": "NI",  "NO_INTERESTED": "NI",
    "NP": "NP",  "DC": "DC",
    "BUSY": "BUSY",  "B": "BUSY",
    "CALLBACK": "CALLBACK",  "CB": "CALLBACK",
    "LBR": "LBR",
    "LIVE": "LIVE",  "L": "LIVE",
    "RINGING": "RI",  "RI": "RI",  "RING": "RI",
}
def normalize_dispo(raw):
    if not raw:
        return "UNKNOWN"
    return DISPO_ALIASES.get(raw.upper().strip(), raw.upper().strip())
# ═══════════════════════════════════════════════════════════════════════════
#  MongoDB Read-Only Validator
# ═══════════════════════════════════════════════════════════════════════════
class MongoValidator:
    FIELDS = [
        'test_id', 'timestamp', 'call_id', 'unique_id', 'lead_id', 'caller_id',
        'freeswitch_uuid', 'audio_file', 'intended_dispo', 'intended_norm',
        'db_unique_id', 'db_lead_id', 'db_caller_id',
        'lead_id_match', 'caller_id_match',
        'db_status_raw', 'db_status_norm', 'db_expected_dispo',
        'db_call_duration', 'db_is_processed', 'db_machine_host',
        'validation_result', 'mismatch_detail', 'lookup_method', 'retries_needed',
    ]
    def __init__(self, validation_log, test_id):
        self.validation_log  = validation_log
        self.test_id         = test_id
        self._lock           = threading.Lock()
        self._client         = None
        self._connected      = False
        self.total_validated = 0
        self.total_match     = 0
        self.total_mismatch  = 0
        self.total_not_found = 0
        self._pending_count  = 0
        self._pending_lock   = threading.Lock()
        self.by_dispo        = defaultdict(lambda: {"intended": 0, "matched": 0, "mismatched": 0, "not_found": 0})
        self.sent_vs_received = defaultdict(lambda: defaultdict(int))
        self._init_log()
        self._connect()
    def _connect(self):
        if not MONGO_AVAILABLE:
            print("  WARNING: pymongo not installed — pip install pymongo --break-system-packages")
            return
        try:
            self._client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000, connectTimeoutMS=8000)
            self._client.admin.command('ping')
            self._connected = True
            print("  ✅ MongoDB connected (validation active)")
        except Exception as e:
            print(f"  WARNING: MongoDB failed: {e}")
            print("  Validation will be skipped this run.")
    def close(self):
        if self._client:
            try: self._client.close()
            except Exception: pass
    def _init_log(self):
        with open(self.validation_log, 'w', newline='') as f:
            csv.writer(f).writerow(self.FIELDS)
    def _write_row(self, row):
        with open(self.validation_log, 'a', newline='') as f:
            csv.DictWriter(f, fieldnames=self.FIELDS, extrasaction='ignore').writerow(row)
    def _query_by_unique_id(self, uid):
        if not self._connected: return None
        try:
            return self._client[MONGO_DB][MONGO_COLLECTION].find_one(
                {"uniqueId": uid},
                {"_id": 0, "uniqueId": 1, "leadId": 1, "callerId": 1, "callId": 1,
                 "expectedDispo": 1, "status": 1, "clientId": 1,
                 "callDuration": 1, "machineHost": 1, "isProcessed": 1, "createdAt": 1}
            )
        except Exception as e:
            print(f"  WARNING: DB query error (uid={uid}): {e}")
            return None
    def _query_by_lead_caller(self, lead_id, caller_id):
        if not self._connected: return None
        try:
            return self._client[MONGO_DB][MONGO_COLLECTION].find_one(
                {"leadId": str(lead_id), "callerId": str(caller_id)},
                {"_id": 0, "uniqueId": 1, "leadId": 1, "callerId": 1, "callId": 1,
                 "expectedDispo": 1, "status": 1, "clientId": 1,
                 "callDuration": 1, "machineHost": 1, "isProcessed": 1, "createdAt": 1},
                sort=[("createdAt", -1)]
            )
        except Exception as e:
            print(f"  WARNING: DB query error (lead={lead_id}, caller={caller_id}): {e}")
            return None
    @property
    def pending_count(self):
        with self._pending_lock:
            return self._pending_count
    def validate_call(self, call_id, unique_id, lead_id, caller_id,
                      freeswitch_uuid, audio_file, intended_dispo):
        with self._pending_lock:
            self._pending_count += 1
        threading.Thread(
            target=self._validate_worker,
            args=(call_id, unique_id, lead_id, caller_id, freeswitch_uuid, audio_file, intended_dispo),
            daemon=True
        ).start()
    def _validate_worker(self, call_id, unique_id, lead_id, caller_id,
                         freeswitch_uuid, audio_file, intended_dispo):
        try:
            intended_norm = normalize_dispo(intended_dispo)
            print(f"  [VALIDATE] call_id={call_id} intended={intended_norm} — waiting {VALIDATION_DELAY}s...")
            time.sleep(VALIDATION_DELAY)
            doc          = None
            retries_used = 0
            method       = "uniqueId"
            for attempt in range(VALIDATION_RETRIES):
                retries_used = attempt
                # Use indexed leadId+callerId query first (fast, no full scan)
                doc = self._query_by_lead_caller(lead_id, caller_id)
                if doc:
                    method = "leadId+callerId (indexed)"
                    break
                if attempt < VALIDATION_RETRIES - 1:
                    print(f"  [VALIDATE] call_id={call_id} not in DB yet — retry {attempt+1}/{VALIDATION_RETRIES} in {VALIDATION_DELAY}s...")
                    time.sleep(VALIDATION_DELAY)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if doc is None:
                row = {
                    'test_id': self.test_id, 'timestamp': ts,
                    'call_id': call_id, 'unique_id': unique_id,
                    'lead_id': lead_id, 'caller_id': caller_id,
                    'freeswitch_uuid': freeswitch_uuid, 'audio_file': audio_file,
                    'intended_dispo': intended_dispo, 'intended_norm': intended_norm,
                    'db_unique_id': '', 'db_lead_id': '', 'db_caller_id': '',
                    'lead_id_match': 'N/A', 'caller_id_match': 'N/A',
                    'db_status_raw': '', 'db_status_norm': '', 'db_expected_dispo': '',
                    'db_call_duration': '', 'db_is_processed': '', 'db_machine_host': '',
                    'validation_result': 'NOT_FOUND_IN_DB',
                    'mismatch_detail': f'No record after {VALIDATION_RETRIES} retries',
                    'lookup_method': 'all_failed', 'retries_needed': retries_used,
                }
                with self._lock:
                    self.total_validated += 1
                    self.total_not_found += 1
                    self.by_dispo[intended_norm]['intended']  += 1
                    self.by_dispo[intended_norm]['not_found'] += 1
                    self.sent_vs_received[intended_norm]['NOT_FOUND'] += 1
                self._write_row(row)
                print(f"  ❌ NOT_FOUND | call_id={call_id} SENT={intended_norm} → NOT_FOUND")
                return
            db_lead   = str(doc.get('leadId',   '')).strip()
            db_caller = str(doc.get('callerId', '')).strip()
            our_lead  = str(lead_id).strip()
            our_call  = str(caller_id).strip()
            lead_ok   = (db_lead   == our_lead)
            caller_ok = (db_caller == our_call)
            id_ok     = lead_ok and caller_ok
            db_status_raw  = doc.get('status') or 'UNKNOWN'
            db_status_norm = normalize_dispo(db_status_raw)
            db_expected    = doc.get('expectedDispo') or ''
            mismatch_parts = []
            if not lead_ok:
                mismatch_parts.append(f"leadId sent='{our_lead}' db='{db_lead}'")
            if not caller_ok:
                mismatch_parts.append(f"callerId sent='{our_call}' db='{db_caller}'")
            if not id_ok:
                result = 'IDENTITY_MISMATCH'
                detail = ' | '.join(mismatch_parts)
                icon   = '⚠️'
            elif db_status_norm == intended_norm:
                result = 'MATCH'
                detail = ''
                icon   = '✅'
            else:
                result = 'MISMATCH'
                detail = f"intended='{intended_norm}' db='{db_status_norm}' (raw='{db_status_raw}') db_expected='{db_expected}'"
                icon   = '❌'
            row = {
                'test_id': self.test_id, 'timestamp': ts,
                'call_id': call_id, 'unique_id': unique_id,
                'lead_id': lead_id, 'caller_id': caller_id,
                'freeswitch_uuid': freeswitch_uuid, 'audio_file': audio_file,
                'intended_dispo': intended_dispo, 'intended_norm': intended_norm,
                'db_unique_id':  doc.get('uniqueId', ''),
                'db_lead_id':    db_lead, 'db_caller_id': db_caller,
                'lead_id_match':   'YES' if lead_ok   else 'NO',
                'caller_id_match': 'YES' if caller_ok else 'NO',
                'db_status_raw': db_status_raw, 'db_status_norm': db_status_norm,
                'db_expected_dispo': db_expected,
                'db_call_duration': doc.get('callDuration', ''),
                'db_is_processed':  doc.get('isProcessed', ''),
                'db_machine_host':  doc.get('machineHost', ''),
                'validation_result': result,
                'mismatch_detail':   detail,
                'lookup_method':     method,
                'retries_needed':    retries_used,
            }
            with self._lock:
                self.total_validated += 1
                self.by_dispo[intended_norm]['intended'] += 1
                if id_ok:
                    self.sent_vs_received[intended_norm][db_status_norm] += 1
                else:
                    self.sent_vs_received[intended_norm]['IDENTITY_MISMATCH'] += 1
                if result == 'MATCH':
                    self.total_match += 1
                    self.by_dispo[intended_norm]['matched'] += 1
                else:
                    self.total_mismatch += 1
                    self.by_dispo[intended_norm]['mismatched'] += 1
            self._write_row(row)
            print(f"  {icon} [{result}] call_id={call_id} | "
                  f"SENT={intended_norm} → DB={db_status_norm} (raw={db_status_raw}) | "
                  f"lead={'OK' if lead_ok else 'FAIL'} caller={'OK' if caller_ok else 'FAIL'}")
        finally:
            with self._pending_lock:
                self._pending_count -= 1
    def wait_for_all(self, timeout_secs=300, hard_stop_event=None):
        deadline = time.time() + timeout_secs
        last = -1
        while time.time() < deadline:
            if hard_stop_event and hard_stop_event.is_set():
                break
            n = self.pending_count
            if n == 0:
                print("  All validations complete.")
                break
            if n != last:
                print(f"  Waiting for {n} validation(s) ({int(deadline - time.time())}s remaining)...")
                last = n
            time.sleep(2)
        left = self.pending_count
        if left > 0:
            print(f"  WARNING: timed out — {left} validation(s) still in flight")
    def get_summary(self):
        with self._lock:
            found = self.total_match + self.total_mismatch
            rate  = round(self.total_match * 100 / found, 2) if found > 0 else 0.0
            return {
                'total_validated':  self.total_validated,
                'total_match':      self.total_match,
                'total_mismatch':   self.total_mismatch,
                'total_not_found':  self.total_not_found,
                'match_rate_pct':   rate,
                'by_dispo':         dict(self.by_dispo),
                'sent_vs_received': {k: dict(v) for k, v in self.sent_vs_received.items()},
            }
    def print_summary(self):
        s = self.get_summary()
        print("\n" + "═" * 70)
        print("  DISPOSITION VALIDATION SUMMARY")
        print("═" * 70)
        print(f"  Test ID         : {self.test_id}")
        print(f"  Total validated : {s['total_validated']}")
        print(f"  ✅ Matched       : {s['total_match']}")
        print(f"  ❌ Mismatched    : {s['total_mismatch']}")
        print(f"  ⚠️  Not in DB     : {s['total_not_found']}")
        print(f"  Match rate      : {s['match_rate_pct']}%")
        svr = s['sent_vs_received']
        if svr:
            print("\n" + "─" * 70)
            print("  SENT vs RECEIVED — What We Sent vs What DB Recorded")
            print("─" * 70)
            all_rx = sorted({rx for m in svr.values() for rx in m})
            sent_w = max(len(d) for d in svr) + 2
            rx_w   = max((len(r) for r in all_rx), default=8) + 2
            rx_w   = max(rx_w, 10)
            hdr = f"  {'SENT':<{sent_w}}"
            for rx in all_rx:
                hdr += f"  {rx:>{rx_w}}"
            hdr += f"  {'TOTAL':>7}"
            print(hdr)
            print("  " + "-" * (sent_w + len(all_rx) * (rx_w + 2) + 9))
            for sent in sorted(svr.keys()):
                rxm   = svr[sent]
                total = sum(rxm.values())
                row   = f"  {sent:<{sent_w}}"
                for rx in all_rx:
                    cnt  = rxm.get(rx, 0)
                    pct  = cnt * 100 / total if total else 0
                    cell = f"{cnt}({pct:.0f}%)"
                    if rx == sent:    cell = f"✅{cell}"
                    elif cnt > 0:     cell = f"❌{cell}"
                    else:             cell = f"  {cell}"
                    row += f"  {cell:>{rx_w+2}}"
                row += f"  {total:>7}"
                print(row)
            print("─" * 70)
            print("\n  PLAIN-ENGLISH BREAKDOWN:")
            for sent in sorted(svr.keys()):
                rxm   = svr[sent]
                total = sum(rxm.values())
                print(f"\n  We sent [{sent}] ({total} calls):")
                for rx, cnt in sorted(rxm.items(), key=lambda x: -x[1]):
                    pct  = cnt * 100 / total if total else 0
                    icon = "✅" if rx == sent else "❌"
                    note = " ← CORRECT" if rx == sent else f" ← WRONG (expected {sent})"
                    print(f"    {icon}  DB recorded [{rx}]  :  {cnt:>4} calls  ({pct:.1f}%){note}")
        print("\n" + "═" * 70)
# ═══════════════════════════════════════════════════════════════════════════
#  Main Load Tester
# ═══════════════════════════════════════════════════════════════════════════
class FreeSWITCHLoadTester:
    MODE_TOTAL_CALLS = "total_calls"
    MODE_DURATION    = "duration"
    MODE_CONTINUOUS  = "continuous"
    def __init__(self, test_id=None, concurrent_calls=50, run_mode=None,
                 total_calls=0, test_duration=600,
                 sbc_address=DEFAULT_SBC, target_extension=DEFAULT_EXT,
                 audio_base_path=DEFAULT_AUDIO,
                 client_id=DEFAULT_CLIENT, campaign=None,
                 long_call_percent=0, selected_folders=None,
                 dispo_ratios=None, batch_size=10):
        # Auto-generate test_id if not provided
        if test_id is None:
            test_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Apply campaign → audio_base_path override
        if campaign is not None:
            camp = campaign.lower()
            if camp in CAMPAIGN_PATHS:
                audio_base_path = CAMPAIGN_PATHS[camp]
        # Derive run_mode from other params when not explicitly set
        if run_mode is None:
            if total_calls > 0:
                run_mode = self.MODE_TOTAL_CALLS
            elif test_duration > 0:
                run_mode = self.MODE_DURATION
            else:
                run_mode = self.MODE_CONTINUOUS
        self.test_id           = test_id
        self.concurrent_calls  = concurrent_calls
        self.run_mode          = run_mode
        self.total_calls_limit = total_calls
        self.test_duration     = test_duration
        self.sbc_address       = sbc_address
        self.target_extension  = target_extension
        self.audio_base_path   = audio_base_path
        self.client_id         = client_id
        self.campaign          = campaign or ""
        self.long_call_percent = long_call_percent
        self.batch_size        = batch_size if batch_size and batch_size > 0 else 10
        # Auto-discover folders if not provided
        if selected_folders is None:
            selected_folders = self.discover_folders_static(audio_base_path)
        self.selected_folders = selected_folders
        # Derive equal-split ratios if not provided
        if dispo_ratios is None:
            n = len(selected_folders)
            per = 100 // n
            dispo_ratios = {f: per for f in selected_folders}
            dispo_ratios[selected_folders[-1]] += 100 - per * n
        self.dispo_ratios = dispo_ratios  # {folder_name: percent_int}
        # Audio discovery
        self.available_folders            = self._discover_folders()
        self.audio_files, self.file_dispo_map = self._discover_audio_files()
        self._check_fix_permissions()
        # Deterministic disposition pool
        self._files_by_dispo = self._build_files_by_dispo()
        self._dispo_pool     = self._build_dispo_pool()
        self._call_counter   = 0
        self._counter_lock   = threading.Lock()
        # Stats
        self.calls_started   = 0
        self.calls_completed = 0
        self.calls_failed    = 0
        self.calls_immediate = 0
        self.calls_timeout   = 0
        self.call_durations  = []
        self.active_calls    = {}
        self._actual_dispo_sent  = defaultdict(int)  # track actual calls sent per dispo
        self._monitor_threads    = []
        self._monitor_lock       = threading.Lock()
        self.stats_lock          = threading.Lock()
        self.stop_event          = threading.Event()
        self._shutdown_requested = False
        self._hard_stop          = threading.Event()
        # Output files
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.stats_file       = f"/tmp/stats_{ts}.csv"
        self.call_log_file    = f"/tmp/calls_{ts}.csv"
        self.headers_log_file = f"/tmp/headers_{ts}.csv"
        self.validation_log   = f"/tmp/validation_{ts}.csv"
        with open(self.headers_log_file, 'w', newline='') as f:
            csv.writer(f).writerow(['test_id', 'call_id', 'lead_id', 'uuid', 'header_name', 'header_value'])
        self._init_csv_files()
        self.validator = MongoValidator(self.validation_log, test_id)
        self._cleanup_old_tmp_files()
    # ── Temp file cleanup ────────────────────────────────────────────────────
    def _cleanup_old_tmp_files(self):
        """Delete /tmp testbed CSV files older than TMP_FILE_MAX_AGE_HOURS."""
        patterns = ["/tmp/stats_*.csv", "/tmp/calls_*.csv",
                    "/tmp/headers_*.csv", "/tmp/validation_*.csv"]
        cutoff = time.time() - TMP_FILE_MAX_AGE_HOURS * 3600
        deleted, skipped = 0, 0
        for pattern in patterns:
            for f in glob.glob(pattern):
                try:
                    if os.path.getmtime(f) < cutoff:
                        os.remove(f)
                        deleted += 1
                    else:
                        skipped += 1
                except OSError:
                    pass
        if deleted:
            print(f"  [Cleanup] Removed {deleted} old tmp file(s) (>{TMP_FILE_MAX_AGE_HOURS}h old), kept {skipped} recent.")
    # ── System health ─────────────────────────────────────────────────────────
    @staticmethod
    def _get_system_health():
        """Return memory usage (MB) and current tmp file count."""
        mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        tmp_patterns = ["/tmp/stats_*.csv", "/tmp/calls_*.csv",
                        "/tmp/headers_*.csv", "/tmp/validation_*.csv"]
        tmp_count = sum(len(glob.glob(p)) for p in tmp_patterns)
        return mem_mb, tmp_count
    # ── Audio discovery ──────────────────────────────────────────────────────
    def _discover_folders(self):
        print(f"  Scanning: {self.audio_base_path}")
        if not os.path.exists(self.audio_base_path):
            raise Exception(f"Path does not exist: {self.audio_base_path}")
        folders = sorted([i for i in os.listdir(self.audio_base_path)
                          if os.path.isdir(os.path.join(self.audio_base_path, i))
                          and i not in EXCLUDED_FOLDERS])
        if not folders:
            raise Exception(f"No subdirectories in {self.audio_base_path}")
        return folders
    @staticmethod
    def discover_folders_static(base_path):
        if not os.path.exists(base_path):
            raise Exception(f"Path does not exist: {base_path}")
        folders = sorted([i for i in os.listdir(base_path)
                          if os.path.isdir(os.path.join(base_path, i))
                          and i not in EXCLUDED_FOLDERS])
        if not folders:
            raise Exception(f"No subdirectories in {base_path}")
        return folders
    def _discover_audio_files(self):
        exts      = ['*.wav', '*.mp3', '*.ogg', '*.aiff', '*.au']
        all_files = []
        dispo_map = {}
        print(f"  Loading audio from: {self.selected_folders}")
        for folder in self.selected_folders:
            folder_path = os.path.join(self.audio_base_path, folder)
            if not os.path.exists(folder_path):
                print(f"  WARNING: Folder not found: {folder}")
                continue
            found = []
            for ext in exts:
                found.extend(glob.glob(os.path.join(folder_path, ext)))
            if not found:
                print(f"  WARNING: No audio files in folder: {folder}")
            else:
                pct = self.dispo_ratios.get(folder, 0)
                print(f"  {folder}: {len(found)} files  (ratio: {pct}%)")
            for fp in found:
                all_files.append(fp)
                dispo_map[fp] = folder
        if not all_files:
            raise Exception(f"No audio files found in: {self.selected_folders}")
        return all_files, dispo_map
    def _check_fix_permissions(self):
        fixed = 0
        for fp in self.audio_files:
            try:
                if not (os.stat(fp).st_mode & stat.S_IROTH):
                    os.chmod(fp, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
                    fixed += 1
            except PermissionError:
                print(f"  WARNING: Cannot fix permissions: {fp}")
            except Exception as e:
                print(f"  WARNING: {fp}: {e}")
        print(f"  Permissions checked ({fixed} fixed)")
    # ── Deterministic disposition pool ──────────────────────────────────────
    def _build_files_by_dispo(self):
        result = defaultdict(list)
        for fp, dispo in self.file_dispo_map.items():
            result[dispo].append(fp)
        return dict(result)
    def _build_dispo_pool(self):
        # Build a base cycle of exactly 100 slots matching configured ratios.
        # Repeat shuffled cycles to fill POOL_SIZE — guarantees exact ratio
        # for every 100 consecutive calls regardless of batch size.
        base_cycle = []
        for dispo, pct in self.dispo_ratios.items():
            base_cycle.extend([dispo] * pct)
        rng = random.Random(hash(self.test_id) & 0xFFFFFFFF)
        pool = []
        repeats = max(1, POOL_SIZE // len(base_cycle))
        for _ in range(repeats):
            cycle = base_cycle.copy()
            rng.shuffle(cycle)
            pool.extend(cycle)
        pool = pool[:POOL_SIZE]
        print(f"\n  Disposition pool built ({len(pool)} slots, cyclic distribution):")
        counts = defaultdict(int)
        for d in pool:
            counts[d] += 1
        for d in self.selected_folders:
            actual_pct = counts[d] * 100 / len(pool)
            target_pct = self.dispo_ratios.get(d, 0)
            print(f"    {d}: {counts[d]} slots ({actual_pct:.1f}% actual vs {target_pct}% target)")
        return pool
    def _pick_dispo_and_file(self):
        with self._counter_lock:
            idx = self._call_counter % len(self._dispo_pool)
            self._call_counter += 1
        intended_dispo = self._dispo_pool[idx]
        with self.stats_lock:
            self._actual_dispo_sent[intended_dispo] += 1
        files = self._files_by_dispo.get(intended_dispo)
        if not files:
            print(f"  WARNING: No files for dispo '{intended_dispo}', using fallback")
            return random.choice(self.audio_files), intended_dispo
        return random.choice(files), intended_dispo
    # ── CSV helpers ──────────────────────────────────────────────────────────
    def _init_csv_files(self):
        with open(self.stats_file, 'w', newline='') as f:
            csv.writer(f).writerow(['test_id', 'timestamp', 'event', 'call_id',
                                    'uuid', 'duration', 'active_calls', 'status'])
        with open(self.call_log_file, 'w', newline='') as f:
            csv.writer(f).writerow(['test_id', 'call_id', 'uuid', 'start_time',
                                    'end_time', 'duration', 'status', 'intended_dispo', 'details'])
    def _log_event(self, event, call_id=None, uuid=None, duration=0, status=""):
        with open(self.stats_file, 'a', newline='') as f:
            csv.writer(f).writerow([self.test_id,
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    event, call_id, uuid, duration,
                                    len(self.active_calls), status])
    def _log_call(self, call_id, uuid, start_time, end_time, duration,
                  status, intended_dispo, details):
        with open(self.call_log_file, 'a', newline='') as f:
            csv.writer(f).writerow([self.test_id, call_id, uuid, start_time,
                                    end_time, duration, status, intended_dispo, details])
    # ── FreeSWITCH helpers ───────────────────────────────────────────────────
    def _get_active_count_from_fs(self):
        try:
            r = subprocess.run(['fs_cli', '-p', 'ClueCon', '-x', 'show calls count'],
                               capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                m = re.search(r'\d+', r.stdout)
                return int(m.group()) if m else 0
        except Exception:
            pass
        return len(self.active_calls)
    def _kill_all_active_calls(self):
        with self.stats_lock:
            uuids = list(self.active_calls.keys())
        if not uuids:
            print("  No active calls to kill.")
            return
        print(f"\n  Killing {len(uuids)} active call(s)...")
        for uuid in uuids:
            try:
                r = subprocess.run(['fs_cli', '-p', 'ClueCon', '-x', f'uuid_kill {uuid}'],
                                   capture_output=True, text=True, timeout=5)
                print(f"  uuid_kill {uuid} -> {r.stdout.strip() or r.stderr.strip() or 'sent'}")
            except Exception as e:
                print(f"  WARNING: uuid_kill failed {uuid}: {e}")
    # ── Call lifecycle ───────────────────────────────────────────────────────
    def start_call(self, call_id):
        intended_dispo = "UNKNOWN"
        start_time     = time.time()
        try:
            audio_file, intended_dispo = self._pick_dispo_and_file()
            audio_stem     = os.path.splitext(os.path.basename(audio_file))[0]
            playback_audio = DISPO_PLAYBACK_OVERRIDE.get(intended_dispo, audio_file)
            lead_id        = random.randint(1000000, 9999999)
            caller_id      = f"{random.choice(AREA_CODES)}{random.choice(AREA_CODES)}{random.randint(1000, 9999)}"
            unique_id      = f"unique-{call_id}-{int(time.time())}"
            call_id_header = f"call-{call_id}-{int(time.time())}"
            headers = [
                ("X-VICIdial-User-Agent",       "VICIdial-SIPP"),
                ("X-VICIdial-Lead-Id",          str(lead_id)),
                ("X-VICIdial-Caller-Id",        caller_id),
                ("X-VICIdial-value",            "DemoValue"),
                ("X-VICIdial-Client-Id",        self.client_id),
                ("X-VICIdial-User-Id",          "agent001"),
                ("X-VICIdial-Campaign-Id",      self.campaign),
                ("X-VICIdial-Unique-Id",        unique_id),
                ("X-VICIdial-Call-Id",          call_id_header),
                ("X-VICIdial-Header-Soundfile", audio_stem),
                ("X-VICIdial-Predicted-Dispo",  intended_dispo),
                ("X-VICIdial-Audio-Path",       audio_file),
            ]
            # Log headers before dial (uuid blank)
            with open(self.headers_log_file, 'a', newline='') as hf:
                w = csv.writer(hf)
                for name, val in headers:
                    w.writerow([self.test_id, call_id, lead_id, "", name, val])
            originate_str = (
                f"originate {{"
                f"sip_h_X-VICIdial-User-Agent=VICIdial-SIPP,"
                f"sip_h_X-VICIdial-Lead-Id={lead_id},"
                f"sip_h_X-VICIdial-Caller-Id={caller_id},"
                f"sip_h_X-VICIdial-value=DemoValue,"
                f"sip_h_X-VICIdial-Client-Id={self.client_id},"
                f"sip_h_X-VICIdial-User-Id=agent001,"
                f"sip_h_X-VICIdial-Campaign-Id={self.campaign},"
                f"sip_h_X-VICIdial-Unique-Id={unique_id},"
                f"sip_h_X-VICIdial-Call-Id={call_id_header},"
                f"sip_h_X-VICIdial-Header-Soundfile={audio_stem},"
                f"sip_h_X-VICIdial-Predicted-Dispo={intended_dispo}"
                f"}}sofia/external/{self.target_extension}@{self.sbc_address} "
                f"&endless_playback({playback_audio})"
            )
            result = subprocess.run(
                ['fs_cli', '-p', 'ClueCon', '-x', originate_str],
                capture_output=True, text=True, timeout=10
            )
            print(f"  [#{call_id}] RC={result.returncode} | "
                  f"OUT={result.stdout.strip()} | ERR={result.stderr.strip()}")
            if result.returncode == 0 and "+OK" in result.stdout:
                uuid = result.stdout.split("+OK ")[1].strip()
                # Log headers with UUID
                with open(self.headers_log_file, 'a', newline='') as hf:
                    w = csv.writer(hf)
                    for name, val in headers:
                        w.writerow([self.test_id, call_id, lead_id, uuid, name, val])
                with self.stats_lock:
                    self.calls_started += 1
                    self.active_calls[uuid] = {
                        'call_id':        call_id,
                        'start_time':     start_time,
                        'uuid':           uuid,
                        'unique_id':      unique_id,
                        'lead_id':        lead_id,
                        'caller_id':      caller_id,
                        'intended_dispo': intended_dispo,
                        'audio_file':     audio_stem,
                    }
                self._log_event("call_started", call_id, uuid, 0, "SUCCESS")
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] Call #{call_id} STARTED | "
                      f"UUID={uuid} | Lead={lead_id} | Caller={caller_id} | "
                      f"UniqueId={unique_id} | Dispo={intended_dispo}")
                t = threading.Thread(
                    target=self._monitor_call,
                    args=(call_id, uuid, start_time, unique_id,
                          lead_id, caller_id, intended_dispo, audio_stem),
                    daemon=True
                )
                with self._monitor_lock:
                    self._monitor_threads.append(t)
                t.start()
                return True
            else:
                raise Exception(result.stderr.strip() or result.stdout.strip() or "Unknown error")
        except Exception as e:
            with self.stats_lock:
                self.calls_failed += 1
            self._log_event("call_failed", call_id, "", 0, f"FAILED: {e}")
            self._log_call(call_id, "", start_time, time.time(), 0,
                           "FAILED_TO_START", intended_dispo, str(e))
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] Call #{call_id} FAILED: {e}")
            return False
    def _monitor_call(self, call_id, uuid, start_time, unique_id,
                      lead_id, caller_id, intended_dispo, audio_stem):
        def record_and_validate(forced=False):
            end_time = time.time()
            duration = int(end_time - start_time)
            with self.stats_lock:
                self.active_calls.pop(uuid, None)
                if duration <= 5:
                    self.calls_immediate += 1
                    status  = "IMMEDIATE_HANGUP"
                    details = f"Ended in {duration}s"
                elif forced:
                    self.calls_timeout += 1
                    status  = "FORCED_HANGUP"
                    details = f"Killed after {duration}s"
                else:
                    self.calls_completed += 1
                    if len(self.call_durations) < 5000:
                        self.call_durations.append(duration)
                    status  = "NORMAL"
                    details = f"Normal ({duration}s)"
            self._log_event("call_completed", call_id, uuid, duration, status)
            self._log_call(call_id, uuid, start_time, end_time, duration,
                           status, intended_dispo, details)
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] Call #{call_id} {status} "
                  f"— {duration}s | dispo={intended_dispo} | queuing validation...")
            self.validator.validate_call(
                call_id=call_id, unique_id=unique_id,
                lead_id=lead_id, caller_id=caller_id,
                freeswitch_uuid=uuid, audio_file=audio_stem,
                intended_dispo=intended_dispo,
            )
        def call_still_up():
            try:
                r = subprocess.run(['fs_cli', '-p', 'ClueCon', '-x', 'show calls'],
                                   capture_output=True, text=True, timeout=5)
                return uuid in r.stdout
            except Exception:
                return False
        try:
            time.sleep(3)
            kill_sent = False
            while True:
                if not call_still_up():
                    record_and_validate(forced=kill_sent)
                    return
                elapsed = time.time() - start_time
                if elapsed > 120 and not kill_sent:
                    print(f"  [{datetime.now().strftime('%H:%M:%S')}] Timeout — killing call #{call_id}")
                    subprocess.run(['fs_cli', '-p', 'ClueCon', '-x', f'uuid_kill {uuid}'],
                                   capture_output=True, timeout=5)
                    kill_sent = True
                    time.sleep(2)
                    continue
                if self.stop_event.is_set() and not kill_sent:
                    subprocess.run(['fs_cli', '-p', 'ClueCon', '-x', f'uuid_kill {uuid}'],
                                   capture_output=True, timeout=5)
                    kill_sent = True
                    time.sleep(2)
                    continue
                if kill_sent and (time.time() - start_time) > 165:
                    print(f"  WARNING: Call #{call_id} still visible 45s after kill — forcing record")
                    record_and_validate(forced=True)
                    return
                time.sleep(5)
        except Exception as e:
            print(f"  Error monitoring call #{call_id}: {e}")
            with self.stats_lock:
                self.active_calls.pop(uuid, None)
            self.validator.validate_call(
                call_id=call_id, unique_id=unique_id,
                lead_id=lead_id, caller_id=caller_id,
                freeswitch_uuid=uuid, audio_file=audio_stem,
                intended_dispo=intended_dispo,
            )
    # ── Call generator ───────────────────────────────────────────────────────
    def call_generator(self):
        call_id    = 0
        test_start = time.time()
        if self.run_mode == self.MODE_DURATION:
            h = self.test_duration // 3600
            m = (self.test_duration % 3600) // 60
            print(f"  Mode: Duration — {h}h {m}m")
        elif self.run_mode == self.MODE_CONTINUOUS:
            print(f"  Mode: Continuous — press Ctrl+C to stop")
        else:
            print(f"  Mode: Total calls — {self.total_calls_limit}")
        print(f"  Batch size: {self.batch_size}")
        while not self.stop_event.is_set():
            if self.run_mode == self.MODE_DURATION:
                if time.time() - test_start >= self.test_duration:
                    print("  Duration reached — stopping call generation.")
                    break
            elif self.run_mode == self.MODE_CONTINUOUS:
                pass  # runs until Ctrl+C sets stop_event
            else:
                with self.stats_lock:
                    if self.calls_started >= self.total_calls_limit:
                        print(f"  All {self.total_calls_limit} calls started — stopping.")
                        break
            with self.stats_lock:
                active_count = len(self.active_calls)
            if active_count >= self.concurrent_calls:
                time.sleep(1)
                continue
            needed = self.concurrent_calls - active_count
            if self.run_mode == self.MODE_TOTAL_CALLS:
                with self.stats_lock:
                    remaining = self.total_calls_limit - self.calls_started
                needed = min(needed, remaining)
                if needed <= 0:
                    time.sleep(1)
                    continue
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] Active: {active_count} | "
                  f"Starting {needed} in batches of {self.batch_size}")
            sent = 0
            while sent < needed and not self.stop_event.is_set():
                batch = min(self.batch_size, needed - sent)
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] Batch #{call_id+1}-#{call_id+batch}")
                for _ in range(batch):
                    call_id += 1
                    threading.Thread(target=self.start_call, args=(call_id,), daemon=True).start()
                    time.sleep(0.05)
                sent += batch
                if sent < needed:
                    time.sleep(1.0)
            time.sleep(2)
    # ── Status reporter ──────────────────────────────────────────────────────
    def status_reporter(self):
        test_start = time.time()
        while not self.stop_event.is_set():
            time.sleep(60)
            elapsed = time.time() - test_start
            if self.run_mode == self.MODE_DURATION and elapsed >= self.test_duration:
                break
            with self.stats_lock:
                active    = len(self.active_calls)
                avg_dur   = (sum(self.call_durations) / len(self.call_durations)
                             if self.call_durations else 0)
                started   = self.calls_started
                completed = self.calls_completed
                immediate = self.calls_immediate
                failed    = self.calls_failed
                timeouts  = self.calls_timeout
            vs = self.validator.get_summary()
            m  = int(elapsed // 60)
            s  = int(elapsed % 60)
            print(f"\n{'='*65}")
            print(f"  STATUS — Test ID: {self.test_id} | {m}m {s}s elapsed")
            print(f"{'='*65}")
            if self.run_mode == self.MODE_TOTAL_CALLS:
                pct = started * 100 / self.total_calls_limit if self.total_calls_limit else 0
                print(f"  Progress:          {started}/{self.total_calls_limit} ({pct:.1f}%)")
            elif self.run_mode == self.MODE_CONTINUOUS:
                print(f"  Progress:          {started} calls (continuous — Ctrl+C to stop)")
            else:
                rem = max(0, self.test_duration - int(elapsed))
                print(f"  Time remaining:    {rem//3600}h {(rem%3600)//60}m")
            print(f"  Active:            {active}/{self.concurrent_calls}")
            print(f"  Started:           {started}")
            print(f"  Completed normal:  {completed}")
            print(f"  Immediate hangups: {immediate}")
            print(f"  Failed:            {failed}")
            print(f"  Timeouts:          {timeouts}")
            print(f"  Avg duration:      {avg_dur:.1f}s")
            if started:
                print(f"  Success rate:      {completed*100/started:.1f}%")
            print(f"  Batch size:        {self.batch_size}")
            print(f"\n  -- Validation Snapshot --")
            print(f"  Validated: {vs['total_validated']} | "
                  f"✅ Match: {vs['total_match']} | "
                  f"❌ Mismatch: {vs['total_mismatch']} | "
                  f"⚠️  Not found: {vs['total_not_found']} | "
                  f"Rate: {vs['match_rate_pct']}%")
            svr = vs.get('sent_vs_received', {})
            if svr:
                print(f"\n  -- Sent vs Received (live) --")
                for sent_d in sorted(svr.keys()):
                    parts = [f"{rx}:{cnt}" for rx, cnt in
                             sorted(svr[sent_d].items(), key=lambda x: -x[1])]
                    print(f"  [{sent_d}] -> {' | '.join(parts)}")
            mem_mb, tmp_count = self._get_system_health()
            print(f"\n  -- System Health --")
            print(f"  Memory (RSS):      {mem_mb:.1f} MB")
            print(f"  Tmp files (/tmp):  {tmp_count} file(s)")
            print(f"{'='*65}\n")
    # ── Signal handling ──────────────────────────────────────────────────────
    def signal_handler(self, signum, frame):
        if not self._shutdown_requested:
            print("\n  Ctrl+C — stopping new calls, killing active, then completing validation...")
            self._shutdown_requested = True
            self.stop_event.set()
        else:
            print("\n  Force quit.")
            self._hard_stop.set()
            sys.exit(0)
    # ── Main test runner ─────────────────────────────────────────────────────
    def run_test(self):
        if self.run_mode == self.MODE_DURATION:
            h, m = self.test_duration // 3600, (self.test_duration % 3600) // 60
            mode_str = f"Duration {h}h {m}m"
        elif self.run_mode == self.MODE_CONTINUOUS:
            mode_str = "Continuous (Ctrl+C to stop)"
        else:
            mode_str = f"Total calls {self.total_calls_limit}"
        print("\n" + "=" * 65)
        print("  FREESWITCH TESTBED — DETERMINISTIC DISPOSITION FRAMEWORK")
        print("=" * 65)
        print(f"  Test ID      : {self.test_id}")
        print(f"  Mode         : {mode_str}")
        print(f"  Concurrency  : {self.concurrent_calls}")
        print(f"  Batch size   : {self.batch_size}")
        print(f"  SBC          : {self.sbc_address}")
        print(f"  Extension    : {self.target_extension}")
        print(f"  Client-Id    : {self.client_id}")
        print(f"  Audio path   : {self.audio_base_path}")
        print(f"  Dispositions : {', '.join(self.selected_folders)}")
        print(f"  Dispo ratios : {self.dispo_ratios}")
        print(f"  Audio files  : {len(self.audio_files)}")
        print(f"  Started      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 65)
        signal.signal(signal.SIGINT,  self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        gen_t = threading.Thread(target=self.call_generator,  daemon=True)
        rep_t = threading.Thread(target=self.status_reporter, daemon=True)
        gen_t.start()
        rep_t.start()
        while gen_t.is_alive():
            gen_t.join(timeout=1.0)
            if self.stop_event.is_set():
                break
        # Wait for active calls to finish naturally so AMD can detect disposition
        print("\n  Call generation complete — waiting for active calls to finish (up to 150s)...")
        deadline = time.time() + 150
        while time.time() < deadline:
            with self.stats_lock:
                active = len(self.active_calls)
            if active == 0:
                break
            time.sleep(2)
        self.stop_event.set()
        self._kill_all_active_calls()
        print("\n  Waiting for monitor threads (up to 30s)...")
        with self._monitor_lock:
            mon = list(self._monitor_threads)
        for t in mon:
            t.join(timeout=30)
        max_wait = VALIDATION_DELAY * (VALIDATION_RETRIES + 1) + 60
        print(f"\n  Waiting up to {max_wait}s for DB validations...")
        self.validator.wait_for_all(timeout_secs=max_wait, hard_stop_event=self._hard_stop)
        self._final_report()
        self.validator.close()
    def _final_report(self):
        final_active = self._get_active_count_from_fs()
        avg_dur = (sum(self.call_durations) / len(self.call_durations)
                   if self.call_durations else 0)
        print("\n" + "=" * 65)
        print("  FINAL REPORT")
        print("=" * 65)
        print(f"  Test ID      : {self.test_id}")
        print(f"  Concurrency  : {self.concurrent_calls}")
        print(f"  Batch size   : {self.batch_size}")
        print(f"  SBC          : {self.sbc_address}")
        print(f"  Client-Id    : {self.client_id}")
        print(f"  Dispositions : {', '.join(self.selected_folders)}")
        print(f"  Dispo ratios : {self.dispo_ratios}")
        print(f"  Audio files  : {len(self.audio_files)}")
        print(f"\n  -- Call Statistics --")
        print(f"  Total started     : {self.calls_started}")
        print(f"  Completed normal  : {self.calls_completed}")
        print(f"  Immediate hangups : {self.calls_immediate}")
        print(f"  Failed to start   : {self.calls_failed}")
        print(f"  Timeouts          : {self.calls_timeout}")
        print(f"  Still active      : {final_active}")
        print(f"  Avg call duration : {avg_dur:.1f}s")
        if self.calls_started > 0:
            print(f"\n  -- Rates --")
            print(f"  Normal completion : {self.calls_completed*100/self.calls_started:.1f}%")
            print(f"  Immediate hangup  : {self.calls_immediate*100/self.calls_started:.1f}%")
            print(f"  Failed to start   : {self.calls_failed*100/self.calls_started:.1f}%")
        print(f"\n  -- Intended Disposition Distribution --")
        total_started = self.calls_started if self.calls_started > 0 else 1
        all_within_tolerance = True
        for dispo, target_pct in sorted(self.dispo_ratios.items()):
            files = len(self._files_by_dispo.get(dispo, []))
            actual_count = self._actual_dispo_sent.get(dispo, 0)
            actual_pct = actual_count * 100 / total_started
            diff = abs(actual_pct - target_pct)
            status = "OK" if diff <= 2.0 else "DRIFT"
            if diff > 2.0:
                all_within_tolerance = False
            print(f"  {dispo:<12}: target={target_pct}%  actual={actual_pct:.1f}%  diff={diff:.1f}%  [{status}]  | {files} audio files")
        if len(self.dispo_ratios) > 1:
            result_label = "PASS — all within ±2%" if all_within_tolerance else "FAIL — some folders drifted >2%"
            print(f"\n  Ratio Tolerance Check : {result_label}")
        self.validator.print_summary()
        mem_mb, tmp_count = self._get_system_health()
        print(f"\n  -- System Health (Final) --")
        print(f"  Memory (RSS):      {mem_mb:.1f} MB")
        print(f"  Tmp files (/tmp):  {tmp_count} file(s)")
        print(f"\n  -- Output Files --")
        print(f"  Stats        : {self.stats_file}")
        print(f"  Call details : {self.call_log_file}")
        print(f"  SIP headers  : {self.headers_log_file}")
        print(f"  Validation   : {self.validation_log}")
        print(f"\n  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 65)
# ═══════════════════════════════════════════════════════════════════════════
#  Campaign helpers
# ═══════════════════════════════════════════════════════════════════════════
def get_campaign_audio_path(campaign):
    """Return the audio base path for a campaign name, or None if unknown.

    Portal usage::
        path = get_campaign_audio_path("solar")
        tester = FreeSWITCHLoadTester(campaign="solar", ...)
    """
    return CAMPAIGN_PATHS.get(campaign.lower()) if campaign else None

# ═══════════════════════════════════════════════════════════════════════════
#  Interactive helpers
# ═══════════════════════════════════════════════════════════════════════════
def _ask_int(prompt, min_val=1, max_val=None):
    while True:
        try:
            val = int(input(prompt).strip())
            if val < min_val:
                print(f"  Must be >= {min_val}")
                continue
            if max_val is not None and val > max_val:
                print(f"  Must be <= {max_val}")
                continue
            return val
        except ValueError:
            print("  Enter a whole number.")
def show_folder_menu(available_folders):
    print("\n" + "=" * 55)
    print("  AVAILABLE DISPOSITION FOLDERS")
    print("=" * 55)
    for i, folder in enumerate(available_folders, 1):
        print(f"  {i:>3}. {folder}")
    print(f"  {len(available_folders)+1:>3}. ALL FOLDERS")
    print("=" * 55)
    while True:
        try:
            raw = input("\n  Select (e.g. 1,3,5 or 'all'): ").strip().lower()
            if raw in ['all', str(len(available_folders) + 1)]:
                return list(available_folders)
            idxs = [int(x.strip()) for x in raw.split(',')]
            sel  = []
            for idx in idxs:
                if 1 <= idx <= len(available_folders):
                    sel.append(available_folders[idx - 1])
                else:
                    raise ValueError(f"No item #{idx}")
            if not sel:
                raise ValueError("Nothing selected")
            return sel
        except (ValueError, IndexError) as e:
            print(f"  Invalid: {e} — try again")
def ask_dispo_ratios(selected_folders):
    if len(selected_folders) == 1:
        return {selected_folders[0]: 100}
    n = len(selected_folders)
    print(f"\n  ┌─ Disposition Ratios ({'  '.join(selected_folders)}) ─┐")
    print(f"  │  Total must sum to 100%                           │")
    print(f"  │  Enter = even split  |  Last folder = auto-fill   │")
    print(f"  │  Type 'r' at any point to restart                 │")
    print(f"  └───────────────────────────────────────────────────┘")
    while True:
        ratios    = {}
        remaining = 100
        restart   = False
        print()
        for i, folder in enumerate(selected_folders):
            # Last folder — auto fill remainder
            if i == n - 1:
                ratios[folder] = remaining
                print(f"  {folder:<14} {remaining:>3}%  ← auto (remaining)")
                continue  # let for loop finish naturally → confirmation runs
            # Show progress bar
            used = 100 - remaining
            bar  = "█" * (used // 5) + "░" * (remaining // 5)
            print(f"  Used: {used:>3}%  [{bar}]  Remaining: {remaining}%")
            while True:
                max_pct = remaining - (n - i - 1)
                raw = input(f"  {folder:<14} (1-{max_pct}%, Enter=even): ").strip()
                # Restart
                if raw.lower() == 'r':
                    print("  Restarting...\n")
                    restart = True
                    break
                # Even split (only on first folder)
                if not raw and i == 0:
                    per    = 100 // n
                    ratios = {f: per for f in selected_folders}
                    ratios[selected_folders[-1]] += 100 - per * n
                    print(f"\n  Even split applied:")
                    for f, p in ratios.items():
                        bar2 = "█" * (p // 5) + "░" * (20 - p // 5)
                        print(f"    {f:<14} {p:>3}%  [{bar2}]")
                    return ratios
                if not raw:
                    print(f"  ✗  Enter a number between 1 and {max_pct}")
                    continue
                try:
                    pct = int(raw)
                except ValueError:
                    print(f"  ✗  Enter a whole number (e.g. 30)")
                    continue
                if pct < 1 or pct > max_pct:
                    print(f"  ✗  Must be 1–{max_pct}  ({n-i-1} folder(s) still need at least 1% each)")
                    continue
                ratios[folder] = pct
                remaining -= pct
                break
            if restart:
                break  # break for loop → outer while restarts
        if restart:
            continue  # restart outer while loop
        # All folders filled — show summary and confirm
        print(f"\n  Summary:")
        for f, p in ratios.items():
            bar = "█" * (p // 5) + "░" * (20 - p // 5)
            print(f"    {f:<14} {p:>3}%  [{bar}]")
        confirm = input("\n  Confirm? (Enter=yes / r=redo): ").strip().lower()
        if confirm != 'r':
            return ratios
        print("  Restarting...\n")
def parse_duration(raw):
    raw   = raw.strip().lower().replace(" ", "")
    h_m   = re.search(r"(\d+)h", raw)
    m_m   = re.search(r"(\d+)m", raw)
    dur_h = int(h_m.group(1)) if h_m else 0
    dur_m = int(m_m.group(1)) if m_m else 0
    if not h_m and not m_m:
        try:    dur_m = int(raw)
        except: dur_h, dur_m = 1, 0
    secs = dur_h * 3600 + dur_m * 60
    return max(secs, 60), dur_h, dur_m
# ═══════════════════════════════════════════════════════════════════════════
#  Entry point
# ═══════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="FreeSWITCH Testbed — Deterministic Disposition Routing & Validation",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument('-s',  '--sbc',         default=None)
    parser.add_argument('-c',  '--concurrent',  type=int, default=None)
    parser.add_argument('-e',  '--extension',   default=None)
    parser.add_argument('-a',  '--audio-path',  default=None, dest='audio_path')
    parser.add_argument('-cp', '--campaign',    default=None)
    parser.add_argument('--client-id',          default=None, dest='client_id')
    parser.add_argument('-b',  '--batch-size',  type=int, default=None, dest='batch_size')
    parser.add_argument('-f',  '--folders',     default=None,
                        help='Comma-separated folder names, e.g. AM,XFER,DNQ')
    parser.add_argument('-r',  '--ratios',      default=None,
                        help='Comma-separated ratios matching --folders, e.g. 60,30,10')
    parser.add_argument('-n',  '--total-calls', type=int, default=None, dest='total_calls',
                        help='Total calls mode: stop after N calls')
    parser.add_argument('-d',  '--duration',    default=None,
                        help='Duration mode: e.g. 1h, 30m, 1h30m')
    parser.add_argument('--continuous', action='store_true', default=False,
                        help='Continuous mode: run until Ctrl+C')
    args = parser.parse_args()

    test_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    print("\n" + "=" * 65)
    print("  FreeSWITCH Testbed — Deterministic Disposition Framework")
    print(f"  Test ID: {test_id}")
    print("=" * 65)

    # ── Audio base path ───────────────────────────────────────────────────────
    if args.campaign:
        camp = args.campaign.lower()
        if camp not in CAMPAIGN_PATHS:
            print(f"  Unknown campaign '{args.campaign}'. Options: {', '.join(CAMPAIGN_PATHS)}")
            sys.exit(1)
        audio_base = CAMPAIGN_PATHS[camp]
        print(f"  Campaign '{camp}' -> {audio_base}")
    elif args.audio_path:
        audio_base = args.audio_path
    else:
        print("\n  Available campaigns:")
        for k, v in CAMPAIGN_PATHS.items():
            print(f"    {k:<12} -> {v}")
        raw_camp = input("\n  Campaign name (or Enter for default path): ").strip().lower()
        if raw_camp and raw_camp in CAMPAIGN_PATHS:
            audio_base = CAMPAIGN_PATHS[raw_camp]
        else:
            audio_base = DEFAULT_AUDIO
        print(f"  Audio path: {audio_base}")

    # ── Folder selection ──────────────────────────────────────────────────────
    available_folders = FreeSWITCHLoadTester.discover_folders_static(audio_base)
    if args.folders:
        requested = [f.strip() for f in args.folders.split(',') if f.strip()]
        bad = [f for f in requested if f not in available_folders]
        if bad:
            print(f"  Unknown folders: {', '.join(bad)}")
            print(f"  Available: {', '.join(available_folders)}")
            sys.exit(1)
        selected_folders = requested
        print(f"\n  Folders: {', '.join(selected_folders)}")
    else:
        selected_folders = show_folder_menu(available_folders)
        print(f"\n  Selected: {', '.join(selected_folders)}")

    # ── Disposition ratios ────────────────────────────────────────────────────
    if args.ratios:
        try:
            raw_ratios = [int(r.strip()) for r in args.ratios.split(',')]
        except ValueError:
            print("  ERROR: --ratios must be integers, e.g. 60,40")
            sys.exit(1)
        if len(raw_ratios) != len(selected_folders):
            print(f"  ERROR: --ratios count ({len(raw_ratios)}) must match --folders count ({len(selected_folders)})")
            sys.exit(1)
        if sum(raw_ratios) != 100:
            print(f"  ERROR: --ratios must sum to 100 (got {sum(raw_ratios)})")
            sys.exit(1)
        dispo_ratios = dict(zip(selected_folders, raw_ratios))
        print(f"  Ratios: {dispo_ratios}")
    else:
        dispo_ratios = ask_dispo_ratios(selected_folders)
        print(f"  Ratios: {dispo_ratios}")

    # ── Connection params ─────────────────────────────────────────────────────
    sbc_address = (args.sbc or
                   input(f"\n  SBC address       (Enter='{DEFAULT_SBC}'): ").strip()
                   or DEFAULT_SBC)
    target_ext  = (args.extension or
                   input(f"  Target extension  (Enter='{DEFAULT_EXT}'): ").strip()
                   or DEFAULT_EXT)
    client_id   = (args.client_id or
                   input(f"  Client-Id         (Enter='{DEFAULT_CLIENT}'): ").strip()
                   or DEFAULT_CLIENT)
    concurrent  = (args.concurrent if args.concurrent is not None
                   else _ask_int("\n  Concurrent calls (e.g. 50): "))
    batch_size  = (args.batch_size if args.batch_size is not None
                   else _ask_int("  Batch size (e.g. 10): "))

    # ── Run mode ──────────────────────────────────────────────────────────────
    if args.total_calls is not None:
        run_mode      = FreeSWITCHLoadTester.MODE_TOTAL_CALLS
        total_calls   = args.total_calls
        test_duration = 0
        mode_label    = f"Total calls -> {total_calls}"
    elif args.continuous:
        run_mode      = FreeSWITCHLoadTester.MODE_CONTINUOUS
        total_calls   = 0
        test_duration = 0
        mode_label    = "Continuous (Ctrl+C to stop)"
    elif args.duration is not None:
        test_duration, dur_h, dur_m = parse_duration(args.duration)
        run_mode      = FreeSWITCHLoadTester.MODE_DURATION
        total_calls   = 0
        mode_label    = f"Duration -> {dur_h}h {dur_m}m ({test_duration}s)"
    else:
        print("\n  Run mode:")
        print("    1. Total calls — stop after N calls")
        print("    2. Duration    — run for a set time (e.g. 1h, 30m)")
        print("    3. Continuous  — run until Ctrl+C")
        mode_choice = _ask_int("  Choose (1, 2 or 3): ", min_val=1, max_val=3)
        if mode_choice == 1:
            run_mode      = FreeSWITCHLoadTester.MODE_TOTAL_CALLS
            total_calls   = _ask_int(f"  Total calls (>= {concurrent}): ", min_val=concurrent)
            test_duration = 0
            mode_label    = f"Total calls -> {total_calls}"
        elif mode_choice == 2:
            run_mode    = FreeSWITCHLoadTester.MODE_DURATION
            total_calls = 0
            print("  Examples: 1h, 2h, 30m, 1h30m")
            dur_raw = input("  Duration: ").strip()
            test_duration, dur_h, dur_m = parse_duration(dur_raw)
            mode_label = f"Duration -> {dur_h}h {dur_m}m ({test_duration}s)"
        else:
            run_mode      = FreeSWITCHLoadTester.MODE_CONTINUOUS
            total_calls   = 0
            test_duration = 0
            mode_label    = "Continuous (Ctrl+C to stop)"

    # ── Confirm & run ─────────────────────────────────────────────────────────
    fully_automated = any([args.sbc, args.concurrent, args.extension, args.client_id,
                           args.folders, args.campaign, args.audio_path,
                           args.total_calls, args.duration, args.continuous])
    print("\n" + "-" * 65)
    print(f"  Test ID      : {test_id}")
    print(f"  Audio path   : {audio_base}")
    print(f"  Dispositions : {', '.join(selected_folders)}")
    print(f"  Ratios       : {dispo_ratios}")
    print(f"  SBC          : {sbc_address}")
    print(f"  Extension    : {target_ext}")
    print(f"  Client-Id    : {client_id}")
    print(f"  Concurrency  : {concurrent}")
    print(f"  Batch size   : {batch_size}")
    print(f"  Mode         : {mode_label}")
    print(f"  Validation   : MongoDB — sent dispo vs DB recorded status")
    print("-" * 65)

    if not fully_automated:
        input("\n  Press Enter to begin (Ctrl+C to abort)...")
    else:
        print("\n  Starting test...")

    try:
        tester = FreeSWITCHLoadTester(
            test_id=test_id,
            concurrent_calls=concurrent,
            run_mode=run_mode,
            total_calls=total_calls,
            test_duration=test_duration,
            sbc_address=sbc_address,
            target_extension=target_ext,
            audio_base_path=audio_base,
            client_id=client_id,
            selected_folders=selected_folders,
            dispo_ratios=dispo_ratios,
            batch_size=batch_size,
        )
        tester.run_test()
    except Exception as e:
        import traceback
        print(f"\n  Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)
if __name__ == "__main__":
    main()
