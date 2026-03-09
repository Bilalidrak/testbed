#!/usr/bin/env python3
import time, random, csv, threading, queue, subprocess, signal, sys, re, os, glob, stat, argparse
from datetime import datetime
from collections import defaultdict

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION — edit these before running
# ══════════════════════════════════════════════════════════════════════════════
MONGO_URI        = "mongodb://idrakuser:123idrak123@65.21.207.117:31023,157.180.55.44:31024,65.109.123.121:31025/idrakdb?authSource=admin&replicaSet=rs0&readPreference=secondary"
MONGO_DB         = "idrakdb"
MONGO_COLLECTION = "Calls"

DEFAULT_SBC_ADDRESS = "sbcserver12.idrakai.com"
DEFAULT_EXTENSION   = "911"
DEFAULT_AUDIO_BASE  = "/usr/share/freeswitch/sounds/happy_test/"
DEFAULT_CLIENT_ID   = "roundrobin"

CAMPAIGN_PATHS = {
    "fe":       "/usr/share/freeswitch/sounds/happy_test_fe/",
    "solar":    "/usr/share/freeswitch/sounds/happy_test_solar/",
    "hi":       "/usr/share/freeswitch/sounds/happy_test_hi/",
    "hw":       "/usr/share/freeswitch/sounds/happy_test_hw/",
    "accident": "/usr/share/freeswitch/sounds/happy_test_acident/",
}

VALIDATION_RETRY_DELAY = 15   # seconds between DB lookup retries
VALIDATION_MAX_RETRIES = 3    # max retries if record not found yet

# ══════════════════════════════════════════════════════════════════════════════
#  Disposition alias normalization
# ══════════════════════════════════════════════════════════════════════════════
DISPO_ALIASES = {
    "RAXFER": "XFER", "XFER": "XFER", "TRANSFER": "XFER",
    "A": "AM", "AM": "AM", "AMD": "AM", "MACHINE": "AM", "SMART_AM": "AM",
    "DNQ": "DNQ", "DNC": "DNC", "D": "DNC",
    "NI": "NI", "NO_INTERESTED": "NI",
    "NP": "NP", "DC": "DC",
    "BUSY": "BUSY", "B": "BUSY",
    "CALLBACK": "CALLBACK", "CB": "CALLBACK",
    "LBR": "LBR",
    "LIVE": "LIVE", "L": "LIVE",
}

def normalize_dispo(dispo):
    if not dispo:
        return "UNKNOWN"
    return DISPO_ALIASES.get(dispo.upper().strip(), dispo.upper().strip())


# ══════════════════════════════════════════════════════════════════════════════
#  MongoDB Read-Only Validator
# ══════════════════════════════════════════════════════════════════════════════
class MongoValidator:
    def __init__(self, validation_log_file):
        self.validation_log    = validation_log_file
        self._lock             = threading.Lock()
        self._client           = None
        self._connected        = False
        self.total_validated   = 0
        self.total_match       = 0
        self.total_mismatch    = 0
        self.total_not_found   = 0
        self._pending_count    = 0
        self._pending_lock     = threading.Lock()

        # Per-disposition counters (old style, kept for compat)
        self.by_dispo = defaultdict(lambda: {
            "intended": 0, "matched": 0, "mismatched": 0, "not_found": 0
        })

        # ── NEW: sent → received matrix ────────────────────────────────────
        # sent_vs_received[sent_dispo][received_dispo] = count
        self.sent_vs_received = defaultdict(lambda: defaultdict(int))

        self._init_log()
        self._connect()

    def _connect(self):
        if not MONGO_AVAILABLE:
            print("WARNING: pymongo not installed. Run: pip install pymongo --break-system-packages")
            return
        try:
            self._client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000, connectTimeoutMS=8000)
            self._client.admin.command('ping')
            self._connected = True
            print("✅ MongoDB connected (read-only validator active)")
        except Exception as e:
            print(f"WARNING: MongoDB connection failed: {e}")
            print("   Validation will be skipped for this run.")

    def close(self):
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass

    def _init_log(self):
        with open(self.validation_log, 'w', newline='') as f:
            csv.writer(f).writerow([
                'timestamp', 'call_id', 'unique_id', 'lead_id', 'caller_id',
                'freeswitch_uuid', 'audio_file', 'predicted_dispo', 'predicted_norm',
                'db_unique_id', 'db_lead_id', 'db_caller_id',
                'lead_id_match', 'caller_id_match',
                'db_status_raw', 'db_status_norm', 'db_expected_dispo',
                'db_call_duration', 'db_is_processed', 'db_machine_host',
                'validation_result', 'mismatch_detail', 'lookup_method', 'retries_needed'
            ])

    def _write_result(self, row):
        fieldnames = [
            'timestamp', 'call_id', 'unique_id', 'lead_id', 'caller_id',
            'freeswitch_uuid', 'audio_file', 'predicted_dispo', 'predicted_norm',
            'db_unique_id', 'db_lead_id', 'db_caller_id',
            'lead_id_match', 'caller_id_match',
            'db_status_raw', 'db_status_norm', 'db_expected_dispo',
            'db_call_duration', 'db_is_processed', 'db_machine_host',
            'validation_result', 'mismatch_detail', 'lookup_method', 'retries_needed'
        ]
        with open(self.validation_log, 'a', newline='') as f:
            csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore').writerow(row)

    def _query_by_unique_id(self, unique_id):
        if not self._connected or not self._client:
            return None
        try:
            col = self._client[MONGO_DB][MONGO_COLLECTION]
            return col.find_one(
                {"uniqueId": unique_id},
                {"_id": 0, "uniqueId": 1, "leadId": 1, "callerId": 1, "callId": 1,
                 "expectedDispo": 1, "status": 1, "clientId": 1,
                 "callDuration": 1, "machineHost": 1, "isProcessed": 1, "createdAt": 1}
            )
        except Exception as e:
            print(f"WARNING: DB query error (uniqueId={unique_id}): {e}")
            return None

    def _query_by_lead_and_caller(self, lead_id, caller_id):
        if not self._connected or not self._client:
            return None
        try:
            col = self._client[MONGO_DB][MONGO_COLLECTION]
            return col.find_one(
                {"leadId": str(lead_id), "callerId": str(caller_id)},
                {"_id": 0, "uniqueId": 1, "leadId": 1, "callerId": 1, "callId": 1,
                 "expectedDispo": 1, "status": 1, "clientId": 1,
                 "callDuration": 1, "machineHost": 1, "isProcessed": 1, "createdAt": 1},
                sort=[("createdAt", -1)]
            )
        except Exception as e:
            print(f"WARNING: DB query error (leadId={lead_id}, callerId={caller_id}): {e}")
            return None

    @property
    def pending_count(self):
        with self._pending_lock:
            return self._pending_count

    def validate_call(self, call_id, unique_id, lead_id, caller_id,
                      freeswitch_uuid, audio_file, predicted_dispo):
        with self._pending_lock:
            self._pending_count += 1
        t = threading.Thread(
            target=self._validate_worker,
            args=(call_id, unique_id, lead_id, caller_id,
                  freeswitch_uuid, audio_file, predicted_dispo),
            daemon=True
        )
        t.start()

    def _validate_worker(self, call_id, unique_id, lead_id, caller_id,
                         freeswitch_uuid, audio_file, predicted_dispo):
        try:
            predicted_norm = normalize_dispo(predicted_dispo)
            print(f"   [VALIDATE] call_id={call_id} unique_id={unique_id} "
                  f"lead_id={lead_id} caller_id={caller_id} "
                  f"predicted={predicted_norm} "
                  f"— waiting {VALIDATION_RETRY_DELAY}s for DB write...")
            time.sleep(VALIDATION_RETRY_DELAY)

            doc           = None
            retries_used  = 0
            lookup_method = "uniqueId"

            for attempt in range(VALIDATION_MAX_RETRIES):
                retries_used = attempt

                doc = self._query_by_unique_id(unique_id)
                if doc:
                    db_lead    = str(doc.get('leadId',   '')).strip()
                    db_caller  = str(doc.get('callerId', '')).strip()
                    our_lead   = str(lead_id).strip()
                    our_caller = str(caller_id).strip()
                    if db_lead == our_lead and db_caller == our_caller:
                        lookup_method = "uniqueId+leadId+callerId"
                        print(f"   [VALIDATE] call_id={call_id} — identity confirmed via uniqueId "
                              f"(attempt {attempt+1})")
                        break
                    else:
                        print(f"   [VALIDATE] call_id={call_id} — uniqueId found but identity mismatch "
                              f"(db_lead={db_lead} vs {our_lead}, "
                              f"db_caller={db_caller} vs {our_caller}) — trying fallback")
                        doc = None

                if doc is None:
                    doc = self._query_by_lead_and_caller(lead_id, caller_id)
                    if doc:
                        lookup_method = "leadId+callerId (fallback)"
                        print(f"   [VALIDATE] call_id={call_id} — found via leadId+callerId fallback "
                              f"(attempt {attempt+1})")
                        break

                if attempt < VALIDATION_MAX_RETRIES - 1:
                    print(f"   [VALIDATE] call_id={call_id} — not in DB yet, "
                          f"retry {attempt+1}/{VALIDATION_MAX_RETRIES} in {VALIDATION_RETRY_DELAY}s...")
                    time.sleep(VALIDATION_RETRY_DELAY)

            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if doc is None:
                row = {
                    'timestamp': ts, 'call_id': call_id,
                    'unique_id': unique_id, 'lead_id': lead_id, 'caller_id': caller_id,
                    'freeswitch_uuid': freeswitch_uuid, 'audio_file': audio_file,
                    'predicted_dispo': predicted_dispo, 'predicted_norm': predicted_norm,
                    'db_unique_id': '', 'db_lead_id': '', 'db_caller_id': '',
                    'lead_id_match': 'N/A', 'caller_id_match': 'N/A',
                    'db_status_raw': '', 'db_status_norm': '', 'db_expected_dispo': '',
                    'db_call_duration': '', 'db_is_processed': '', 'db_machine_host': '',
                    'validation_result': 'NOT_FOUND_IN_DB',
                    'mismatch_detail': (f'No record after {VALIDATION_MAX_RETRIES} retries '
                                        f'(uniqueId={unique_id}, leadId={lead_id}, callerId={caller_id})'),
                    'lookup_method': 'all_failed',
                    'retries_needed': retries_used,
                }
                with self._lock:
                    self.total_validated += 1
                    self.total_not_found += 1
                    self.by_dispo[predicted_norm]['intended'] += 1
                    self.by_dispo[predicted_norm]['not_found'] += 1
                    self.sent_vs_received[predicted_norm]['NOT_FOUND_IN_DB'] += 1
                self._write_result(row)
                print(f"   ❌ NOT_FOUND | call_id={call_id} "
                      f"SENT={predicted_norm} → DB=NOT_FOUND | "
                      f"unique_id={unique_id} lead_id={lead_id} caller_id={caller_id}")
                return

            db_lead_id   = str(doc.get('leadId',   '')).strip()
            db_caller_id = str(doc.get('callerId', '')).strip()
            our_lead     = str(lead_id).strip()
            our_caller   = str(caller_id).strip()

            lead_id_match   = (db_lead_id   == our_lead)
            caller_id_match = (db_caller_id == our_caller)
            identity_ok     = lead_id_match and caller_id_match

            db_status_raw  = doc.get('status') or 'UNKNOWN'
            db_status_norm = normalize_dispo(db_status_raw)
            db_expected    = doc.get('expectedDispo') or ''

            mismatch_parts = []
            if not lead_id_match:
                mismatch_parts.append(f"leadId sent='{our_lead}' db='{db_lead_id}'")
            if not caller_id_match:
                mismatch_parts.append(f"callerId sent='{our_caller}' db='{db_caller_id}'")

            if not identity_ok:
                validation_result = 'IDENTITY_MISMATCH'
                mismatch_detail   = ' | '.join(mismatch_parts)
                icon              = '⚠️  IDENTITY_MISMATCH'
            elif db_status_norm == predicted_norm:
                validation_result = 'MATCH'
                mismatch_detail   = ''
                icon              = '✅ MATCH'
            else:
                validation_result = 'MISMATCH'
                mismatch_detail   = (f"predicted='{predicted_norm}' "
                                     f"db_recorded='{db_status_norm}' (raw='{db_status_raw}') "
                                     f"db_expected='{db_expected}'")
                icon              = '❌ MISMATCH'

            row = {
                'timestamp': ts, 'call_id': call_id,
                'unique_id': unique_id, 'lead_id': lead_id, 'caller_id': caller_id,
                'freeswitch_uuid': freeswitch_uuid, 'audio_file': audio_file,
                'predicted_dispo': predicted_dispo, 'predicted_norm': predicted_norm,
                'db_unique_id':  doc.get('uniqueId', ''),
                'db_lead_id':    db_lead_id,
                'db_caller_id':  db_caller_id,
                'lead_id_match':   'YES' if lead_id_match   else 'NO',
                'caller_id_match': 'YES' if caller_id_match else 'NO',
                'db_status_raw': db_status_raw, 'db_status_norm': db_status_norm,
                'db_expected_dispo': db_expected,
                'db_call_duration': doc.get('callDuration', ''),
                'db_is_processed':  doc.get('isProcessed', ''),
                'db_machine_host':  doc.get('machineHost', ''),
                'validation_result': validation_result,
                'mismatch_detail':   mismatch_detail,
                'lookup_method':     lookup_method,
                'retries_needed':    retries_used,
            }

            with self._lock:
                self.total_validated += 1
                self.by_dispo[predicted_norm]['intended'] += 1
                # ── Record sent → received mapping ─────────────────────────
                if identity_ok:
                    self.sent_vs_received[predicted_norm][db_status_norm] += 1
                else:
                    self.sent_vs_received[predicted_norm]['IDENTITY_MISMATCH'] += 1

                if validation_result == 'MATCH':
                    self.total_match += 1
                    self.by_dispo[predicted_norm]['matched'] += 1
                elif validation_result == 'IDENTITY_MISMATCH':
                    self.total_mismatch += 1
                    self.by_dispo[predicted_norm]['mismatched'] += 1
                else:
                    self.total_mismatch += 1
                    self.by_dispo[predicted_norm]['mismatched'] += 1

            self._write_result(row)

            # ── Per-call inline result — always shows SENT → DB ────────────
            match_sym = "✅" if validation_result == 'MATCH' else ("⚠️ " if validation_result == 'IDENTITY_MISMATCH' else "❌")
            print(f"   {match_sym} [{validation_result}] call_id={call_id} "
                  f"| SENT={predicted_norm} → DB={db_status_norm} (raw={db_status_raw}) "
                  f"| expected_in_db={db_expected} "
                  f"| lead={our_lead}({'OK' if lead_id_match else 'FAIL'}) "
                  f"caller={our_caller}({'OK' if caller_id_match else 'FAIL'}) "
                  f"| via={lookup_method}")

        finally:
            with self._pending_lock:
                self._pending_count -= 1

    def wait_for_all(self, timeout_secs=300, hard_stop_event=None):
        deadline = time.time() + timeout_secs
        last_reported = -1
        while time.time() < deadline:
            if hard_stop_event and hard_stop_event.is_set():
                break
            remaining = self.pending_count
            if remaining == 0:
                print("   All validations complete.")
                break
            if remaining != last_reported:
                print(f"   Waiting for {remaining} validation(s) "
                      f"({int(deadline - time.time())}s remaining)...")
                last_reported = remaining
            time.sleep(2)
        leftover = self.pending_count
        if leftover > 0:
            print(f"   WARNING: timed out — {leftover} validation(s) still in flight")

    def get_summary(self):
        with self._lock:
            found      = self.total_match + self.total_mismatch
            match_rate = round(self.total_match * 100 / found, 2) if found > 0 else 0.0
            return {
                'total_validated':  self.total_validated,
                'total_match':      self.total_match,
                'total_mismatch':   self.total_mismatch,
                'total_not_found':  self.total_not_found,
                'match_rate_pct':   match_rate,
                'by_dispo':         dict(self.by_dispo),
                'sent_vs_received': {k: dict(v) for k, v in self.sent_vs_received.items()},
            }

    def print_summary(self):
        s = self.get_summary()
        total = s['total_validated'] or 1

        print("\n" + "═" * 70)
        print("  DISPOSITION VALIDATION SUMMARY")
        print("═" * 70)
        print(f"  Total validated : {s['total_validated']}")
        print(f"  ✅ Matched       : {s['total_match']}")
        print(f"  ❌ Mismatched    : {s['total_mismatch']}")
        print(f"  ⚠️  Not in DB     : {s['total_not_found']}")
        print(f"  Match rate      : {s['match_rate_pct']}%")

        # ── Sent vs Received matrix ────────────────────────────────────────
        svr = s['sent_vs_received']
        if svr:
            print("\n" + "─" * 70)
            print("  SENT vs RECEIVED — What We Sent vs What DB Recorded")
            print("─" * 70)

            # Collect all unique received dispositions across all sent dispos
            all_received = sorted({rx for rx_map in svr.values() for rx in rx_map})

            # Header row
            sent_col_w = max(len(d) for d in svr) + 2
            rx_col_w   = max((len(r) for r in all_received), default=8) + 2
            rx_col_w   = max(rx_col_w, 10)

            header = f"  {'SENT (predicted)':<{sent_col_w}}"
            for rx in all_received:
                flag = "✅" if True else ""   # mark correct column per row below
                header += f"  {rx:>{rx_col_w}}"
            header += f"  {'TOTAL':>7}"
            print(header)
            print("  " + "-" * (sent_col_w + len(all_received) * (rx_col_w + 2) + 9))

            for sent_dispo in sorted(svr.keys()):
                rx_map    = svr[sent_dispo]
                row_total = sum(rx_map.values())
                row_str   = f"  {sent_dispo:<{sent_col_w}}"
                for rx in all_received:
                    count = rx_map.get(rx, 0)
                    pct   = count * 100 / row_total if row_total else 0
                    # Mark the cell that matches (correct dispo)
                    cell  = f"{count} ({pct:.0f}%)"
                    if rx == sent_dispo:
                        cell = f"✅{cell}"
                    elif count > 0:
                        cell = f"❌{cell}"
                    else:
                        cell = f"  {cell}"
                    row_str += f"  {cell:>{rx_col_w+2}}"
                row_str += f"  {row_total:>7}"
                print(row_str)

            print("─" * 70)

            # Plain-English interpretation
            print("\n  PLAIN-ENGLISH BREAKDOWN:")
            for sent_dispo in sorted(svr.keys()):
                rx_map    = svr[sent_dispo]
                row_total = sum(rx_map.values())
                correct   = rx_map.get(sent_dispo, 0)
                print(f"\n  We sent [{sent_dispo}] ({row_total} calls):")
                for rx, count in sorted(rx_map.items(), key=lambda x: -x[1]):
                    pct  = count * 100 / row_total if row_total else 0
                    icon = "✅" if rx == sent_dispo else "❌"
                    note = " ← CORRECT" if rx == sent_dispo else f" ← WRONG (expected {sent_dispo})"
                    print(f"    {icon}  DB recorded [{rx}]  :  {count:>4} calls  ({pct:.1f}%){note}")

        print("\n" + "═" * 70)


# ══════════════════════════════════════════════════════════════════════════════
#  Main Load Tester
# ══════════════════════════════════════════════════════════════════════════════
class FreeSWITCHLoadTester:

    MODE_TOTAL_CALLS = "total_calls"
    MODE_DURATION    = "duration"

    def __init__(self, concurrent_calls, run_mode, total_calls, test_duration,
                 sbc_address, target_extension, audio_base_path, client_id,
                 selected_folders, batch_size):
        self.concurrent_calls  = concurrent_calls
        self.run_mode          = run_mode
        self.total_calls_limit = total_calls
        self.test_duration     = test_duration
        self.sbc_address       = sbc_address
        self.target_extension  = target_extension
        self.audio_base_path   = audio_base_path
        self.client_id         = client_id
        self.selected_folders  = selected_folders
        self.batch_size        = batch_size

        self.available_folders                = self.discover_folders()
        self.audio_files, self.file_dispo_map = self.discover_audio_files()
        self.check_and_fix_permissions()

        self.calls_started   = 0
        self.calls_completed = 0
        self.calls_failed    = 0
        self.calls_immediate = 0
        self.calls_timeout   = 0
        self.call_durations  = []
        self.active_calls    = {}

        self._monitor_threads = []
        self._monitor_lock    = threading.Lock()

        self.stats_lock          = threading.Lock()
        self.stop_event          = threading.Event()
        self.call_queue          = queue.Queue()
        self._shutdown_requested = False
        self._hard_stop          = threading.Event()

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.headers_log_file = f"/tmp/headers_log_{ts}.csv"
        self.stats_file       = f"/tmp/python_load_test_{ts}.csv"
        self.call_log_file    = f"/tmp/call_details_{ts}.csv"
        self.validation_log   = f"/tmp/validation_results_{ts}.csv"

        with open(self.headers_log_file, 'w', newline='') as f:
            csv.writer(f).writerow(['call_id', 'lead_id', 'uuid', 'header_name', 'header_value'])
        self._init_csv_files()

        self.validator = MongoValidator(self.validation_log)

    def discover_folders(self):
        print(f"Scanning: {self.audio_base_path}")
        if not os.path.exists(self.audio_base_path):
            raise Exception(f"Path does not exist: {self.audio_base_path}")
        folders = sorted([i for i in os.listdir(self.audio_base_path)
                          if os.path.isdir(os.path.join(self.audio_base_path, i))])
        if not folders:
            raise Exception(f"No subdirectories in {self.audio_base_path}")
        return folders

    @staticmethod
    def discover_folders_static(audio_base_path):
        if not os.path.exists(audio_base_path):
            raise Exception(f"Path does not exist: {audio_base_path}")
        folders = sorted([i for i in os.listdir(audio_base_path)
                          if os.path.isdir(os.path.join(audio_base_path, i))])
        if not folders:
            raise Exception(f"No subdirectories in {audio_base_path}")
        return folders

    def discover_audio_files(self):
        exts = ['*.wav', '*.mp3', '*.ogg', '*.aiff', '*.au']
        audio_files = []
        dispo_map   = {}
        print(f"Scanning selected folders: {self.selected_folders}")
        for folder in self.selected_folders:
            folder_path = os.path.join(self.audio_base_path, folder)
            if not os.path.exists(folder_path):
                print(f"WARNING: Folder not found: {folder}")
                continue
            found = []
            for ext in exts:
                found.extend(glob.glob(os.path.join(folder_path, ext)))
            print(f"   {folder}: {len(found)} files")
            for fp in found:
                audio_files.append(fp)
                dispo_map[fp] = folder
        if not audio_files:
            raise Exception(f"No audio files found in: {self.selected_folders}")
        print(f"Total audio files: {len(audio_files)}")
        for fp in sorted(audio_files):
            print(f"   {os.path.basename(fp)} -> {dispo_map[fp]}")
        return audio_files, dispo_map

    def check_and_fix_permissions(self):
        print(f"\nChecking permissions for {len(self.audio_files)} files...")
        fixed = 0
        for fp in self.audio_files:
            try:
                if not (os.stat(fp).st_mode & stat.S_IROTH):
                    os.chmod(fp, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
                    fixed += 1
            except PermissionError:
                print(f"WARNING: Cannot fix permissions: {fp}")
            except Exception as e:
                print(f"WARNING: {fp}: {e}")
        print(f"Permissions OK ({fixed} fixed)")

    def _init_csv_files(self):
        with open(self.stats_file, 'w', newline='') as f:
            csv.writer(f).writerow(['timestamp', 'event', 'call_id', 'uuid', 'duration', 'active_calls', 'status'])
        with open(self.call_log_file, 'w', newline='') as f:
            csv.writer(f).writerow(['call_id', 'uuid', 'start_time', 'end_time', 'duration', 'status', 'details'])

    def _log_event(self, event, call_id=None, uuid=None, duration=0, status=""):
        with open(self.stats_file, 'a', newline='') as f:
            csv.writer(f).writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                     event, call_id, uuid, duration, len(self.active_calls), status])

    def _log_call_completion(self, call_id, uuid, start_time, end_time, duration, status, details):
        with open(self.call_log_file, 'a', newline='') as f:
            csv.writer(f).writerow([call_id, uuid, start_time, end_time, duration, status, details])

    def _get_active_calls_count(self):
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
            print("No active calls to kill.")
            return
        print(f"\nKilling {len(uuids)} active call(s) to trigger hangup detection + validation...")
        for uuid in uuids:
            try:
                r = subprocess.run(
                    ['fs_cli', '-p', 'ClueCon', '-x', f'uuid_kill {uuid}'],
                    capture_output=True, text=True, timeout=5
                )
                print(f"  uuid_kill {uuid} -> {r.stdout.strip() or r.stderr.strip() or 'sent'}")
            except Exception as e:
                print(f"  WARNING: uuid_kill failed for {uuid}: {e}")

    def start_call(self, call_id):
        try:
            start_time      = time.time()
            audio_file      = random.choice(self.audio_files)
            audio_filename  = os.path.splitext(os.path.basename(audio_file))[0]
            predicted_dispo = self.file_dispo_map.get(audio_file, 'Unknown')

            lead_id        = random.randint(1000000, 9999999)
            caller_id      = f"900{1000000 + call_id}"
            unique_id      = f"unique-{call_id}-{int(time.time())}"
            call_id_header = f"call-{call_id}-{int(time.time())}"

            raw_headers = [
                ("X-VICIdial-User-Agent",       "VICIdial-SIPP"),
                ("X-VICIdial-Lead-Id",          str(lead_id)),
                ("X-VICIdial-Caller-Id",        caller_id),
                ("X-VICIdial-value",            "DemoValue"),
                ("X-VICIdial-Client-Id",        self.client_id),
                ("X-VICIdial-User-Id",          "agent001"),
                ("X-VICIdial-Campaign-Id",      "camp01"),
                ("X-VICIdial-Unique-Id",        unique_id),
                ("X-VICIdial-Call-Id",          call_id_header),
                ("X-VICIdial-Header-Soundfile", audio_filename),
                ("X-VICIdial-Predicted-Dispo",  predicted_dispo),
                ("X-VICIdial-Audio-Path",       audio_file),
            ]

            with open(self.headers_log_file, 'a', newline='') as hf:
                w = csv.writer(hf)
                for name, value in raw_headers:
                    w.writerow([call_id, lead_id, "", name, value])

            cmd = [
                'fs_cli', '-p', 'ClueCon', '-x',
                (f"originate {{"
                 f"sip_h_X-VICIdial-User-Agent=VICIdial-SIPP,"
                 f"sip_h_X-VICIdial-Lead-Id={lead_id},"
                 f"sip_h_X-VICIdial-Caller-Id={caller_id},"
                 f"sip_h_X-VICIdial-value=DemoValue,"
                 f"sip_h_X-VICIdial-Client-Id={self.client_id},"
                 f"sip_h_X-VICIdial-User-Id=agent001,"
                 f"sip_h_X-VICIdial-Campaign-Id=camp01,"
                 f"sip_h_X-VICIdial-Unique-Id={unique_id},"
                 f"sip_h_X-VICIdial-Call-Id={call_id_header},"
                 f"sip_h_X-VICIdial-Header-Soundfile={audio_filename},"
                 f"sip_h_X-VICIdial-Predicted-Dispo={predicted_dispo}"
                 f"}}sofia/external/{self.target_extension}@{self.sbc_address} "
                 f"&endless_playback({audio_file})")
            ]

            print(f"DEBUG originate: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            print(f"  RC: {result.returncode} | OUT: {result.stdout.strip()} | ERR: {result.stderr.strip()}")

            if result.returncode == 0 and "+OK" in result.stdout:
                uuid = result.stdout.split("+OK ")[1].strip()

                with open(self.headers_log_file, 'a', newline='') as hf:
                    w = csv.writer(hf)
                    for name, value in raw_headers:
                        w.writerow([call_id, lead_id, uuid, name, value])

                with self.stats_lock:
                    self.calls_started += 1
                    self.active_calls[uuid] = {
                        'call_id':         call_id,
                        'start_time':      start_time,
                        'uuid':            uuid,
                        'unique_id':       unique_id,
                        'lead_id':         lead_id,
                        'caller_id':       caller_id,
                        'predicted_dispo': predicted_dispo,
                        'audio_file':      audio_filename,
                    }

                self._log_event("call_started", call_id, uuid, 0, "SUCCESS")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Call #{call_id} started | "
                      f"UUID: {uuid} | LeadId: {lead_id} | CallerId: {caller_id} | "
                      f"UniqueId: {unique_id} | Sending Dispo: {predicted_dispo}")

                t = threading.Thread(
                    target=self._monitor_call,
                    args=(call_id, uuid, start_time, unique_id,
                          lead_id, caller_id, predicted_dispo, audio_filename),
                    daemon=True
                )
                with self._monitor_lock:
                    self._monitor_threads.append(t)
                t.start()
                return True
            else:
                raise Exception(result.stderr.strip() or result.stdout.strip())

        except Exception as e:
            with self.stats_lock:
                self.calls_failed += 1
            self._log_event("call_failed", call_id, "", 0, f"FAILED: {e}")
            self._log_call_completion(call_id, "", start_time, time.time(), 0, "FAILED_TO_START", str(e))
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Call #{call_id} FAILED: {e}")
            return False

    def _monitor_call(self, call_id, uuid, start_time, unique_id,
                      lead_id, caller_id, predicted_dispo, audio_filename):
        def _record_and_validate(forced=False):
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
                    if len(self.call_durations) < 1000:
                        self.call_durations.append(duration)
                    status  = "NORMAL"
                    details = f"Normal completion ({duration}s)"

            self._log_event("call_completed", call_id, uuid, duration, status)
            self._log_call_completion(call_id, uuid, start_time, end_time,
                                      duration, status, details)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Call #{call_id} {status} "
                  f"— {duration}s | lead={lead_id} caller={caller_id} "
                  f"| sent_dispo={predicted_dispo} | queuing validation...")

            self.validator.validate_call(
                call_id=call_id,
                unique_id=unique_id,
                lead_id=lead_id,
                caller_id=caller_id,
                freeswitch_uuid=uuid,
                audio_file=audio_filename,
                predicted_dispo=predicted_dispo,
            )

        def _call_still_up():
            try:
                r = subprocess.run(['fs_cli', '-p', 'ClueCon', '-x', 'show calls'],
                                   capture_output=True, text=True, timeout=5)
                return uuid in r.stdout
            except Exception:
                return False

        try:
            time.sleep(3)
            forced_kill_sent = False

            while True:
                if not _call_still_up():
                    _record_and_validate(forced=forced_kill_sent)
                    return

                elapsed = time.time() - start_time

                if elapsed > 120 and not forced_kill_sent:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Timeout — "
                          f"killing call #{call_id} uuid={uuid}")
                    subprocess.run(
                        ['fs_cli', '-p', 'ClueCon', '-x', f'uuid_kill {uuid}'],
                        capture_output=True, timeout=5
                    )
                    forced_kill_sent = True
                    time.sleep(2)
                    continue

                if self.stop_event.is_set() and not forced_kill_sent:
                    subprocess.run(
                        ['fs_cli', '-p', 'ClueCon', '-x', f'uuid_kill {uuid}'],
                        capture_output=True, timeout=5
                    )
                    forced_kill_sent = True
                    time.sleep(2)
                    continue

                if forced_kill_sent and (time.time() - start_time) > 155:
                    print(f"WARNING: Call #{call_id} still visible 30s after kill — forcing record")
                    _record_and_validate(forced=True)
                    return

                time.sleep(5)

        except Exception as e:
            print(f"Error monitoring call #{call_id}: {e}")
            with self.stats_lock:
                self.active_calls.pop(uuid, None)
            self.validator.validate_call(
                call_id=call_id, unique_id=unique_id, lead_id=lead_id,
                caller_id=caller_id, freeswitch_uuid=uuid,
                audio_file=audio_filename, predicted_dispo=predicted_dispo,
            )

    def _call_generator(self):
        call_id    = 0
        test_start = time.time()

        if self.run_mode == self.MODE_DURATION:
            h = self.test_duration // 3600
            m = (self.test_duration % 3600) // 60
            print(f"Starting: Duration mode — running for {h}h {m}m")
        else:
            print(f"Starting: Total-calls mode — {self.total_calls_limit} calls "
                  f"({self.concurrent_calls} concurrent)")
        print(f"Batch size: {self.batch_size}")

        while not self.stop_event.is_set():
            if self.run_mode == self.MODE_DURATION:
                if time.time() - test_start >= self.test_duration:
                    print("Duration reached - stopping call generation.")
                    break
            else:
                with self.stats_lock:
                    started = self.calls_started
                if started >= self.total_calls_limit:
                    print(f"All {self.total_calls_limit} calls started - stopping.")
                    break

            active_count = len(self.active_calls)
            if active_count >= self.concurrent_calls:
                time.sleep(2)
                continue

            needed = self.concurrent_calls - active_count
            if self.run_mode == self.MODE_TOTAL_CALLS:
                with self.stats_lock:
                    remaining = self.total_calls_limit - self.calls_started
                needed = min(needed, remaining)
                if needed <= 0:
                    time.sleep(2)
                    continue

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Active: {active_count} | "
                  f"Scheduling {needed} in batches of {self.batch_size}")

            calls_scheduled = 0
            while calls_scheduled < needed and not self.stop_event.is_set():
                batch = min(self.batch_size, needed - calls_scheduled)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Batch of {batch} "
                      f"(#{call_id+1}-#{call_id+batch})")
                for _ in range(batch):
                    call_id += 1
                    threading.Thread(target=self.start_call, args=(call_id,), daemon=True).start()
                    time.sleep(0.05)
                calls_scheduled += batch
                if calls_scheduled < needed:
                    time.sleep(1.0)

            time.sleep(2)

    def _status_reporter(self):
        test_start = time.time()
        while not self.stop_event.is_set():
            time.sleep(60)
            elapsed = time.time() - test_start
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)

            with self.stats_lock:
                active    = len(self.active_calls)
                avg_dur   = sum(self.call_durations) / len(self.call_durations) if self.call_durations else 0
                s_rate    = self.calls_completed * 100 / self.calls_started if self.calls_started else 0
                i_rate    = self.calls_immediate * 100 / self.calls_started if self.calls_started else 0
                started   = self.calls_started
                completed = self.calls_completed
                immediate = self.calls_immediate
                failed    = self.calls_failed
                timeouts  = self.calls_timeout

            vs = self.validator.get_summary()

            print(f"\n{'='*65}")
            print(f"=== STATUS REPORT - {minutes}m {seconds}s elapsed ===")
            print(f"{'='*65}")
            if self.run_mode == self.MODE_TOTAL_CALLS:
                pct = started * 100 / self.total_calls_limit if self.total_calls_limit else 0
                print(f"  Progress:          {started}/{self.total_calls_limit} calls ({pct:.1f}%)")
            else:
                rem = max(0, self.test_duration - int(elapsed))
                print(f"  Time remaining:    {rem//3600}h {(rem%3600)//60}m")
            print(f"  Active calls:      {active}/{self.concurrent_calls}")
            print(f"  Total started:     {started}")
            print(f"  Completed normal:  {completed}")
            print(f"  Immediate hangups: {immediate}")
            print(f"  Failed to start:   {failed}")
            print(f"  Timeouts:          {timeouts}")
            print(f"  Avg call duration: {avg_dur:.1f}s")
            print(f"  Success rate:      {s_rate:.1f}%")
            print(f"  Immediate rate:    {i_rate:.1f}%")
            print(f"  Dispositions:      {', '.join(self.selected_folders)}")
            print(f"  Batch size:        {self.batch_size}")

            print(f"\n  -- Validation Snapshot --")
            print(f"  Validated: {vs['total_validated']} | ✅ Match: {vs['total_match']} | "
                  f"❌ Mismatch: {vs['total_mismatch']} | ⚠️  Not found: {vs['total_not_found']} | "
                  f"Rate: {vs['match_rate_pct']}%")

            # Quick inline sent→received snapshot
            svr = vs.get('sent_vs_received', {})
            if svr:
                print(f"\n  -- Sent vs Received (live snapshot) --")
                for sent_d in sorted(svr.keys()):
                    rx_map = svr[sent_d]
                    parts  = [f"{rx}:{cnt}" for rx, cnt in sorted(rx_map.items(), key=lambda x: -x[1])]
                    print(f"  Sent [{sent_d}] -> {' | '.join(parts)}")

            print(f"{'='*65}\n")
            if self.run_mode == self.MODE_DURATION and elapsed >= self.test_duration:
                break

    def signal_handler(self, signum, frame):
        if not self._shutdown_requested:
            print("\nCtrl+C — stopping calls, killing active calls, then completing validation...")
            self._shutdown_requested = True
            self.stop_event.set()
        else:
            print("\nForce quit.")
            self._hard_stop.set()
            sys.exit(0)

    def run_test(self):
        if self.run_mode == self.MODE_DURATION:
            h = self.test_duration // 3600
            m = (self.test_duration % 3600) // 60
            mode_str = f"Duration -> {h}h {m}m"
        else:
            mode_str = f"Total calls -> {self.total_calls_limit}"

        print("\n" + "=" * 65)
        print("FREESWITCH LOAD TEST WITH REAL-TIME DISPOSITION VALIDATION")
        print("=" * 65)
        print(f"  Mode:          {mode_str}")
        print(f"  Concurrency:   {self.concurrent_calls} simultaneous calls")
        print(f"  Batch size:    {self.batch_size}")
        print(f"  SBC:           {self.sbc_address}")
        print(f"  Extension:     {self.target_extension}")
        print(f"  Client-Id:     {self.client_id}")
        print(f"  Audio path:    {self.audio_base_path}")
        print(f"  Dispositions:  {', '.join(self.selected_folders)}")
        print(f"  Audio files:   {len(self.audio_files)}")
        print(f"  Validation:    Sent dispo vs DB recorded status, per call")
        print(f"  Started:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 65)

        signal.signal(signal.SIGINT,  self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        gen_thread      = threading.Thread(target=self._call_generator,  daemon=True)
        reporter_thread = threading.Thread(target=self._status_reporter, daemon=True)
        gen_thread.start()
        reporter_thread.start()

        while gen_thread.is_alive():
            gen_thread.join(timeout=1.0)
            if self.stop_event.is_set():
                print("\nStop signal — halting call generation.")
                break

        self.stop_event.set()
        self._kill_all_active_calls()

        print("\nWaiting for monitor threads to confirm hangups (up to 30s)...")
        with self._monitor_lock:
            mon_threads = list(self._monitor_threads)
        for t in mon_threads:
            t.join(timeout=30)

        max_val_wait = VALIDATION_RETRY_DELAY * (VALIDATION_MAX_RETRIES + 1) + 60
        print(f"\nAll calls ended — waiting up to {max_val_wait}s for DB validations...")
        self.validator.wait_for_all(
            timeout_secs=max_val_wait,
            hard_stop_event=self._hard_stop
        )

        self._generate_final_report()
        self.validator.close()

    def _generate_final_report(self):
        final_active = self._get_active_calls_count()
        avg_dur      = sum(self.call_durations) / len(self.call_durations) if self.call_durations else 0

        mode_str = (f"Duration ({self.test_duration//3600}h {(self.test_duration%3600)//60}m)"
                    if self.run_mode == self.MODE_DURATION
                    else f"Total Calls ({self.total_calls_limit})")

        print("\n" + "=" * 65)
        print("FINAL COMPREHENSIVE REPORT")
        print("=" * 65)
        print(f"\n  Run mode:    {mode_str}")
        print(f"  Concurrency: {self.concurrent_calls}")
        print(f"  Batch size:  {self.batch_size}")
        print(f"  SBC:         {self.sbc_address}")
        print(f"  Client-Id:   {self.client_id}")
        print(f"  Dispositions:{', '.join(self.selected_folders)}")
        print(f"  Audio files: {len(self.audio_files)}")
        print(f"\n  -- Call Statistics --")
        print(f"  Total started:     {self.calls_started}")
        print(f"  Completed normal:  {self.calls_completed}")
        print(f"  Immediate hangups: {self.calls_immediate}")
        print(f"  Failed to start:   {self.calls_failed}")
        print(f"  Timeouts:          {self.calls_timeout}")
        print(f"  Still active:      {final_active}")
        print(f"  Avg call duration: {avg_dur:.1f}s")

        if self.calls_started > 0:
            print(f"\n  -- Rates --")
            print(f"  Normal completion: {self.calls_completed*100/self.calls_started:.1f}%")
            print(f"  Immediate hangup:  {self.calls_immediate*100/self.calls_started:.1f}%")
            print(f"  Failed to start:   {self.calls_failed*100/self.calls_started:.1f}%")

        print(f"\n  -- Disposition Breakdown (Audio Files) --")
        dispo_counts = {}
        for fp in self.audio_files:
            d = self.file_dispo_map[fp]
            dispo_counts[d] = dispo_counts.get(d, 0) + 1
        for d, c in sorted(dispo_counts.items()):
            print(f"  {d}: {c} files")

        # Full sent vs received summary (the main new section)
        self.validator.print_summary()

        print(f"\n  -- Output Files --")
        print(f"  Statistics:          {self.stats_file}")
        print(f"  Call details:        {self.call_log_file}")
        print(f"  SIP headers:         {self.headers_log_file}")
        print(f"  Validation results:  {self.validation_log}")
        print(f"\n  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 65)


# ══════════════════════════════════════════════════════════════════════════════
#  Interactive helpers
# ══════════════════════════════════════════════════════════════════════════════
def _ask_int(prompt, min_val=1, max_val=None):
    while True:
        try:
            val = int(input(prompt).strip())
            if val < min_val:
                print(f"  Please enter a value >= {min_val}")
                continue
            if max_val is not None and val > max_val:
                print(f"  Please enter a value <= {max_val}")
                continue
            return val
        except ValueError:
            print("  Invalid - please enter a whole number.")

def show_folder_menu(available_folders):
    print("\n" + "=" * 50)
    print("AVAILABLE DISPOSITION FOLDERS")
    print("=" * 50)
    for i, folder in enumerate(available_folders, 1):
        print(f"  {i:>2}. {folder}")
    print(f"  {len(available_folders)+1:>2}. ALL FOLDERS")
    print("=" * 50)
    while True:
        try:
            raw = input("\nSelect folders (e.g. 1,3,5 or 'all'): ").strip().lower()
            if raw in ['all', str(len(available_folders)+1)]:
                return available_folders
            idxs = [int(x.strip()) for x in raw.split(',')]
            sel  = []
            for idx in idxs:
                if 1 <= idx <= len(available_folders):
                    sel.append(available_folders[idx-1])
                else:
                    raise ValueError(f"No folder #{idx}")
            if not sel:
                raise ValueError("Nothing selected")
            return sel
        except (ValueError, IndexError) as e:
            print(f"  Invalid: {e} - try again")


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="FreeSWITCH Testbed — Disposition Validation Framework",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('-s',  '--sbc',         default=None)
    parser.add_argument('-c',  '--concurrent',  type=int, default=None)
    parser.add_argument('-e',  '--extension',   default=None)
    parser.add_argument('-a',  '--audio-path',  default=None, dest='audio_path')
    parser.add_argument('-cp', '--campaign',    default=None)
    parser.add_argument('--client-id',          default=None, dest='client_id')
    parser.add_argument('-b',  '--batch-size',  type=int, default=None, dest='batch_size')
    parser.add_argument('-f',  '--folders',     default=None,
                        help='Comma-separated folder names, e.g. --folders AM,XFER,DNQ')
    args = parser.parse_args()

    print("\n" + "=" * 65)
    print("  FreeSWITCH Testbed - Disposition Validation Framework")
    print("=" * 65)

    if args.campaign:
        if args.campaign.lower() in CAMPAIGN_PATHS:
            audio_base = CAMPAIGN_PATHS[args.campaign.lower()]
            print(f"  Campaign '{args.campaign}' -> {audio_base}")
        else:
            print(f"  Unknown campaign '{args.campaign}'. Available: {', '.join(CAMPAIGN_PATHS)}")
            sys.exit(1)
    elif args.audio_path:
        audio_base = args.audio_path
    else:
        print("\n  Available campaigns:")
        for k, v in CAMPAIGN_PATHS.items():
            print(f"    {k:<10} -> {v}")
        campaign_input = input("\nCampaign name (or Enter for default): ").strip().lower()
        if campaign_input and campaign_input in CAMPAIGN_PATHS:
            audio_base = CAMPAIGN_PATHS[campaign_input]
        else:
            audio_base = DEFAULT_AUDIO_BASE
        print(f"  Audio path: {audio_base}")

    available_folders = FreeSWITCHLoadTester.discover_folders_static(audio_base)

    if args.folders:
        requested = [f.strip() for f in args.folders.split(',') if f.strip()]
        invalid   = [f for f in requested if f not in available_folders]
        if invalid:
            print(f"  ERROR: Unknown folders: {', '.join(invalid)}")
            sys.exit(1)
        selected_folders = requested
        print(f"\n  Folders: {', '.join(selected_folders)}")
    else:
        selected_folders = show_folder_menu(available_folders)
        print(f"\n  Selected: {', '.join(selected_folders)}")

    sbc_address = args.sbc or input(f"\nSBC address      (Enter='{DEFAULT_SBC_ADDRESS}'): ").strip() or DEFAULT_SBC_ADDRESS
    target_ext  = args.extension or input(f"Target extension (Enter='{DEFAULT_EXTENSION}'): ").strip() or DEFAULT_EXTENSION
    client_id   = args.client_id or input(f"Client-Id        (Enter='{DEFAULT_CLIENT_ID}'): ").strip() or DEFAULT_CLIENT_ID

    concurrent  = args.concurrent  if args.concurrent  is not None else _ask_int("\nConcurrent calls (e.g. 50): ")
    batch_size  = args.batch_size  if args.batch_size  is not None else _ask_int("Batch size (e.g. 10): ")

    print("\n  Run mode:")
    print("    1. Total calls  - stop after N calls")
    print("    2. Duration     - run for a set time")
    mode_choice = _ask_int("Choose (1 or 2): ", min_val=1, max_val=2)

    if mode_choice == 1:
        run_mode      = FreeSWITCHLoadTester.MODE_TOTAL_CALLS
        total_calls   = _ask_int(f"Total calls (>= {concurrent}): ", min_val=concurrent)
        test_duration = 0
        mode_label    = f"Total calls -> {total_calls}"
    else:
        run_mode    = FreeSWITCHLoadTester.MODE_DURATION
        total_calls = 0
        print("\n  Duration examples: 1h, 2h, 30m, 1h30m")
        dur_raw = input("  Duration: ").strip().lower().replace(" ", "")
        import re as _re
        h_match = _re.search(r"(\d+)h", dur_raw)
        m_match = _re.search(r"(\d+)m", dur_raw)
        dur_h = int(h_match.group(1)) if h_match else 0
        dur_m = int(m_match.group(1)) if m_match else 0
        if not h_match and not m_match:
            try:    dur_m, dur_h = int(dur_raw), 0
            except: dur_h, dur_m = 1, 0
        test_duration = dur_h * 3600 + dur_m * 60
        if test_duration < 60:
            test_duration, dur_h, dur_m = 60, 0, 1
        mode_label = f"Duration -> {dur_h}h {dur_m}m ({test_duration}s)"

    print("\n" + "-" * 65)
    print(f"  Audio path:   {audio_base}")
    print(f"  Dispositions: {', '.join(selected_folders)}")
    print(f"  SBC:          {sbc_address}")
    print(f"  Extension:    {target_ext}")
    print(f"  Client-Id:    {client_id}")
    print(f"  Concurrency:  {concurrent}")
    print(f"  Batch size:   {batch_size}")
    print(f"  Run mode:     {mode_label}")
    print(f"  Validation:   Sent dispo vs DB recorded status, per call")
    print("-" * 65)

    fully_automated = any([args.sbc, args.concurrent, args.extension,
                           args.client_id, args.folders, args.campaign, args.audio_path])
    if not fully_automated:
        input("\nPress Enter to begin (Ctrl+C to abort)...")
    else:
        print("\nStarting test...")

    try:
        tester = FreeSWITCHLoadTester(
            concurrent_calls=concurrent, run_mode=run_mode,
            total_calls=total_calls, test_duration=test_duration,
            sbc_address=sbc_address, target_extension=target_ext,
            audio_base_path=audio_base, client_id=client_id,
            selected_folders=selected_folders, batch_size=batch_size,
        )
        tester.run_test()
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
