# !/usr/bin/env python3
import time
import random
import argparse
import csv
import threading
import queue
import subprocess
import signal
import sys
import re
import os
import glob
import stat
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


class FreeSWITCHLoadTester:
    def __init__(self, concurrent_calls=50, test_duration=600, sbc_address="sbcserver12.idrakai.com",
                 target_extension="911", audio_base_path="/usr/share/freeswitch/sounds/happy_test/",
                 client_id="roundrobin", campaign="camp01", long_call_percent=0, selected_folders=None, batch_size=10):

        self.concurrent_calls = concurrent_calls
        self.test_duration = test_duration
        self.sbc_address = sbc_address
        self.target_extension = target_extension
        self.client_id = client_id
        self.campaign = campaign
        self.long_call_percent = long_call_percent
        self.selected_folders = selected_folders or []
        self.batch_size = batch_size
        # Determine audio path based on campaign
        campaign_audio_path = get_campaign_audio_path(campaign)
        print(f"📁 Campaign '{campaign}' , output campaign_audio_path ")
        if campaign_audio_path:
            self.audio_base_path = campaign_audio_path
            print(f"📁 Campaign '{campaign}' mapped to audio path: {self.audio_base_path}")
        else:
            self.audio_base_path = audio_base_path
            print(f"📁 Using provided audio path: {self.audio_base_path}")

        # Only discover and process audio files if folders are selected
        if self.selected_folders:
            self.available_folders = self.discover_folders()
            self.audio_files, self.file_dispo_map = self.discover_audio_files()
            self.check_and_fix_permissions()

            # Split into long and short calls (you can customize this logic)
            self.long_calls, self.short_calls = self.categorize_calls()
        else:
            # Just discover folders, don't process audio files yet
            self.available_folders = self.discover_folders()
            self.audio_files = []
            self.file_dispo_map = {}
            self.long_calls = []
            self.short_calls = []

        # Stats tracking
        self.calls_started = 0
        self.calls_completed = 0
        self.calls_failed = 0
        self.calls_immediate = 0
        self.calls_timeout = 0
        self.call_durations = []
        self.active_calls = {}

        self.stats_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.call_queue = queue.Queue()

        # Only initialize CSV files if we have selected folders
        if self.selected_folders:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Initialize log files
            self.headers_log_file = f"/tmp/headers_log_{timestamp}.csv"
            with open(self.headers_log_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['call_id', 'lead_id', 'uuid', 'header_name', 'header_value'])

            self.stats_file = f"/tmp/python_load_test_{timestamp}.csv"
            self.call_log_file = f"/tmp/call_details_{timestamp}.csv"

            self.init_csv_files()
        else:
            # Placeholder values when no folders selected
            self.headers_log_file = None
            self.stats_file = None
            self.call_log_file = None

    def discover_folders(self):
        """Discover all subdirectories in the audio base path."""
        print(f"🔍 Scanning for subdirectories in: {self.audio_base_path}")

        if not os.path.exists(self.audio_base_path):
            raise Exception(f"Audio base path does not exist: {self.audio_base_path}")

        folders = []
        for item in os.listdir(self.audio_base_path):
            item_path = os.path.join(self.audio_base_path, item)
            if os.path.isdir(item_path):
                folders.append(item)

        if not folders:
            raise Exception(f"No subdirectories found in {self.audio_base_path}")

        folders.sort()
        return folders

    @staticmethod
    def discover_folders_static(audio_base_path):
        """Static method to discover folders without creating a full tester object."""
        print(f"🔍 Scanning for subdirectories in: {audio_base_path}")

        if not os.path.exists(audio_base_path):
            raise Exception(f"Audio base path does not exist: {audio_base_path}")

        folders = []
        for item in os.listdir(audio_base_path):
            item_path = os.path.join(audio_base_path, item)
            if os.path.isdir(item_path):
                folders.append(item)

        if not folders:
            raise Exception(f"No subdirectories found in {audio_base_path}")

        folders.sort()
        return folders

    def discover_audio_files(self):
        """Discover audio files only in selected folders."""
        audio_extensions = ['*.wav', '*.mp3', '*.ogg', '*.aiff', '*.au']
        audio_files = []
        file_dispo_map = {}

        print(f"🎵 Scanning for audio files in selected folders: {self.selected_folders}")

        for folder in self.selected_folders:
            folder_path = os.path.join(self.audio_base_path, folder)
            if not os.path.exists(folder_path):
                print(f"⚠️  Warning: Folder {folder} not found, skipping...")
                continue

            folder_files = []
            for extension in audio_extensions:
                pattern = os.path.join(folder_path, extension)
                files = glob.glob(pattern)
                folder_files.extend(files)

            print(f"   📁 {folder}: Found {len(folder_files)} audio files")
            for file_path in folder_files:
                audio_files.append(file_path)
                file_dispo_map[file_path] = folder  # Use folder name as disposition

        if not audio_files:
            raise Exception(f"No audio files found in selected folders: {self.selected_folders}")

        print(f"📊 Total audio files discovered: {len(audio_files)}")
        for file_path in sorted(audio_files):
            disposition = file_dispo_map[file_path]
            print(f"   {os.path.basename(file_path)} → Disposition: {disposition}")

        return audio_files, file_dispo_map

    def check_and_fix_permissions(self):
        """Check and fix permissions for all discovered audio files."""
        print(f"\n🔒 Checking permissions for {len(self.audio_files)} audio files...")

        fixed_count = 0
        for file_path in self.audio_files:
            try:
                # Check if file is readable
                current_permissions = os.stat(file_path).st_mode

                # Check if file is readable by FreeSWITCH (others can read)
                if not (current_permissions & stat.S_IROTH):
                    print(f"   Fixing permissions for: {os.path.basename(file_path)}")
                    # Set permissions to 644 (owner: rw, group: r, others: r)
                    os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
                    fixed_count += 1

            except PermissionError:
                print(f"⚠️  Cannot fix permissions for {file_path} - run as root or fix manually")
            except Exception as e:
                print(f"⚠️  Error checking {file_path}: {e}")

        if fixed_count > 0:
            print(f"✅ Fixed permissions for {fixed_count} files")
        else:
            print("✅ All files have correct permissions")

    def categorize_calls(self):
        """Categorize audio files into long and short calls based on disposition."""
        long_calls = []
        short_calls = []

        for file_path in self.audio_files:
            disposition = self.file_dispo_map[file_path].lower()

            # Categorize based on disposition - customize as needed
            if disposition in ['transfer', 'xfer', 'raxfer', 'long', 'dnc', 'no_interested']:
                long_calls.append(file_path)
            else:
                short_calls.append(file_path)

        # Ensure we have files in both categories
        if not long_calls:
            long_calls = self.audio_files[:len(self.audio_files) // 2] if len(
                self.audio_files) > 1 else self.audio_files
        if not short_calls:
            short_calls = self.audio_files[len(self.audio_files) // 2:] if len(
                self.audio_files) > 1 else self.audio_files

        print(f"\n📊 Call categorization:")
        print(f"   Long calls ({len(long_calls)}): {[os.path.basename(f) for f in long_calls]}")
        print(f"   Short calls ({len(short_calls)}): {[os.path.basename(f) for f in short_calls]}")

        return long_calls, short_calls

    def init_csv_files(self):
        with open(self.stats_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'event', 'call_id', 'uuid', 'duration', 'active_calls', 'status'])

        with open(self.call_log_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['call_id', 'uuid', 'start_time', 'end_time', 'duration', 'status', 'details'])

    def log_event(self, event, call_id=None, uuid=None, duration=0, status=""):
        active_count = len(self.active_calls)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.stats_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, event, call_id, uuid, duration, active_count, status])

    def log_call_completion(self, call_id, uuid, start_time, end_time, duration, status, details):
        with open(self.call_log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([call_id, uuid, start_time, end_time, duration, status, details])

    def get_active_calls_count(self):
        try:
            result = subprocess.run(['fs_cli', '-p', 'ClueCon', '-x', 'show calls count'],
                                    capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                match = re.search(r'\d+', result.stdout)
                return int(match.group()) if match else 0
            return 0
        except:
            return len(self.active_calls)

    def start_call(self, call_id):
        """Start a single call via FreeSWITCH, log every SIP header we send."""
        try:
            start_time = time.time()

            # 1) pick audio file
            if random.uniform(0, 100) < self.long_call_percent:
                audio_file = random.choice(self.long_calls)
            else:
                audio_file = random.choice(self.short_calls)

            audio_filename = os.path.splitext(os.path.basename(audio_file))[0]

            # 2) Get disposition from our mapping
            predicted_dispo = self.file_dispo_map.get(audio_file, 'Unknown')

            # 3) build the header list
            lead_id = random.randint(1000000, 9999999)
            area_codes = [
                '365', '201', '202', '203', '205', '206', '207', '208', '209', '210', '212', '213', '214', '215', '216', '217', '218', '219', '224', '225', '228', '229', '231', '234', '239', '240', '248', '251', '252', '253', '254', '256', '260', '262', '267', '269', '270', '276', '281', '301', '302', '303', '304', '305', '307', '308', '309', '310', '312', '313', '314', '315', '316', '317', '318', '319', '320', '321', '323', '325', '330', '331', '334', '336', '337', '339', '346', '347', '351', '352', '360', '361', '364', '369', '380', '385', '386', '401', '402', '404', '405', '406', '407', '408', '409', '410', '412', '413', '414', '415', '417', '419', '423', '424', '425', '430', '432', '434', '435', '440', '442', '443', '447', '458', '463', '469', '470', '475', '478', '479', '480', '484', '501', '502', '503', '504', '505', '507', '508', '509', '510', '512', '513', '515', '516', '517', '518', '520', '530', '531', '534', '539', '540', '541', '551', '559', '561', '562', '563', '564', '567', '570', '571', '573', '574', '575', '580', '585', '586', '601', '602', '603', '605', '606', '607', '608', '609', '610', '612', '614', '615', '616', '617', '618', '619', '620', '623', '626', '628', '629', '630', '631', '636', '641', '646', '650', '651', '657', '660', '661', '662', '667', '669', '678', '681', '682', '701', '702', '703', '704', '706', '707', '708', '712', '713', '714', '715', '716', '717', '718', '719', '720', '724', '725', '727', '731', '732', '734', '737', '740', '743', '747', '754', '757', '760', '762', '763', '765', '769', '770', '771', '772', '773', '774', '775', '779', '781', '785', '786', '801', '802', '803', '804', '805', '806', '808', '810', '812', '813', '814', '815', '816', '817', '818', '828', '830', '831', '832', '843', '845', '847', '848', '850', '854', '856', '857', '858', '859', '860', '862', '863', '864', '865', '870', '872', '878', '901', '903', '904', '906', '907', '908', '909', '910', '912', '913', '914', '915', '916', '917', '918', '919', '920', '925', '928', '929', '931', '936', '937', '938', '940', '941', '947', '949', '951', '952', '954', '956', '959', '970', '971', '972', '973', '975', '978', '979', '980', '984', '985', '989'
            ]
            caller_id = f"{random.choice(area_codes)}{random.choice(area_codes)}{random.randint(1000, 9999)}"
            unique_id = f"unique-{call_id}-{int(time.time())}"
            call_id_header = f"call-{call_id}-{int(time.time())}"

            raw_headers = [
                ("X-VICIdial-User-Agent", "VICIdial-SIPP"),
                ("X-VICIdial-Lead-Id", str(lead_id)),
                ("X-VICIdial-Caller-Id", caller_id),
                ("X-VICIdial-value", "DemoValue"),
                ("X-VICIdial-Client-Id", self.client_id),
                ("X-VICIdial-User-Id", "agent001"),
                ("X-VICIdial-Campaign-Id", self.campaign),
                ("X-VICIdial-Unique-Id", unique_id),
                ("X-VICIdial-Call-Id", call_id_header),
                ("X-VICIdial-Header-Soundfile", audio_filename),
                ("X-VICIdial-Predicted-Dispo", predicted_dispo),
                ("X-VICIdial-Audio-Path", audio_file),  # Optional: full path for debugging
            ]

            # 4) log headers (before dialing, uuid blank)
            with open(self.headers_log_file, 'a', newline='') as hf:
                writer = csv.writer(hf)
                for name, value in raw_headers:
                    writer.writerow([call_id, lead_id, "", name, value])

            cmd = [
                'fs_cli', '-p', 'ClueCon', '-x',
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
                f"sip_h_X-VICIdial-Header-Soundfile={audio_filename},"
                f"sip_h_X-VICIdial-Predicted-Dispo={predicted_dispo}"
                f"}}sofia/external/{self.target_extension}@{self.sbc_address} "
                f"&endless_playback({audio_file})"
            ]

            # debug
            print("DEBUG originate command:", " ".join(cmd))

            # 5) run
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            print("Return code:", result.returncode)
            print("Stdout:", result.stdout.strip())
            print("Stderr:", result.stderr.strip())

            # 6) on success, grab UUID
            if result.returncode == 0 and "+OK" in result.stdout:
                uuid = result.stdout.split("+OK ")[1].strip()

                # amend header log with uuid
                with open(self.headers_log_file, 'a', newline='') as hf:
                    writer = csv.writer(hf)
                    for name, value in raw_headers:
                        writer.writerow([call_id, lead_id, uuid, name, value])

                # stats
                with self.stats_lock:
                    self.calls_started += 1
                    self.active_calls[uuid] = {
                        'call_id': call_id,
                        'start_time': start_time,
                        'uuid': uuid
                    }
                self.log_event("call_started", call_id, uuid, 0, "SUCCESS")
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Call #{call_id} started – UUID: {uuid} (Dispo: {predicted_dispo})")

                # monitor
                threading.Thread(
                    target=self.monitor_call,
                    args=(call_id, uuid, start_time),
                    daemon=True
                ).start()

                return True

            else:
                raise Exception(result.stderr.strip() or result.stdout.strip())

        except Exception as e:
            with self.stats_lock:
                self.calls_failed += 1

            self.log_event("call_failed", call_id, "", 0, f"FAILED: {e}")
            self.log_call_completion(
                call_id, "", start_time, time.time(),
                0, "FAILED_TO_START", str(e)
            )
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Call #{call_id} FAILED: {e}")
            return False

    def monitor_call(self, call_id, uuid, start_time):
        try:
            time.sleep(3)
            while not self.stop_event.is_set():
                try:
                    result = subprocess.run(['fs_cli', '-p', 'ClueCon', '-x', 'show calls'],
                                            capture_output=True, text=True, timeout=5)

                    if uuid not in result.stdout:
                        end_time = time.time()
                        duration = int(end_time - start_time)

                        with self.stats_lock:
                            if uuid in self.active_calls:
                                del self.active_calls[uuid]

                            if duration <= 5:
                                self.calls_immediate += 1
                                status = "IMMEDIATE_HANGUP"
                                details = f"Call ended in {duration}s - likely SBC rejection"
                            elif duration > 120:
                                self.calls_timeout += 1
                                status = "TIMEOUT"
                                details = f"Call exceeded timeout threshold ({duration}s)"
                            else:
                                self.calls_completed += 1
                                self.call_durations.append(duration)
                                status = "NORMAL"
                                details = f"Normal completion ({duration}s)"

                        self.log_event("call_completed", call_id, uuid, duration, status)
                        self.log_call_completion(call_id, uuid, start_time, end_time, duration, status, details)
                        print(
                            f"[{datetime.now().strftime('%H:%M:%S')}] Call #{call_id} {status} - Duration: {duration}s")
                        break

                    elapsed = time.time() - start_time
                    if elapsed > 120:
                        subprocess.run(['fs_cli', '-p', 'ClueCon', '-x', f'uuid_kill {uuid}'],
                                       capture_output=True, timeout=5)
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Forced hangup for call #{call_id}")
                        break

                    time.sleep(5)
                except:
                    break
        except Exception as e:
            print(f"Error monitoring call {call_id}: {e}")

    def call_generator(self):
        """Maintain exactly self.concurrent_calls active calls at all times.
        Starts calls in custom batch sizes with 1-second pause between batches."""
        call_id = 0
        test_start = time.time()

        print(f"🚀 Starting call generator for {self.test_duration} seconds...")
        print(f"📦 Starting calls in batches of {self.batch_size} with 1-second pause between batches")

        while not self.stop_event.is_set():
            # stop once test duration is up
            if time.time() - test_start >= self.test_duration:
                print("⏰ Test duration reached. Stopping call generation.")
                break

            active_count = len(self.active_calls)
            if active_count < self.concurrent_calls:
                needed = self.concurrent_calls - active_count

                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Active: {active_count} | Starting {needed} calls in batches of {self.batch_size}")

                # Start calls in custom batch sizes
                calls_started_this_round = 0
                while calls_started_this_round < needed and not self.stop_event.is_set():
                    # Determine batch size (custom size or remaining calls if less)
                    current_batch_size = min(self.batch_size, needed - calls_started_this_round)

                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] 📦 Starting batch of {current_batch_size} calls (Call #{call_id + 1} to #{call_id + current_batch_size})")

                    # Start batch of calls rapidly (no delay between calls in the batch)
                    for i in range(current_batch_size):
                        call_id += 1
                        threading.Thread(target=self.start_call, args=(call_id,), daemon=True).start()
                        time.sleep(0.05)  # Minimal delay to avoid overwhelming the system

                    calls_started_this_round += current_batch_size

                    # Wait 1 second between batches (unless this was the last batch)
                    if calls_started_this_round < needed:
                        print(
                            f"[{datetime.now().strftime('%H:%M:%S')}] ⏱️  Batch complete, waiting 1 second before next batch...")
                        time.sleep(1.0)

            # Short sleep before re-checking
            time.sleep(2)

    def status_reporter(self):
        test_start = time.time()

        while not self.stop_event.is_set():
            time.sleep(60)

            elapsed = time.time() - test_start
            if elapsed >= self.test_duration:
                break

            with self.stats_lock:
                active_count = len(self.active_calls)
                avg_duration = sum(self.call_durations) / len(self.call_durations) if self.call_durations else 0

                minutes = int(elapsed // 60)
                seconds = int(elapsed % 60)

                success_rate = (self.calls_completed * 100 / self.calls_started) if self.calls_started > 0 else 0
                immediate_rate = (self.calls_immediate * 100 / self.calls_started) if self.calls_started > 0 else 0

                print(f"\n=== STATUS REPORT - {minutes}m{seconds}s ===")
                print(f"Active Calls: {active_count}/{self.concurrent_calls}")
                print(f"Total Started: {self.calls_started}")
                print(f"Completed Normally: {self.calls_completed}")
                print(f"Immediate Hangups: {self.calls_immediate}")
                print(f"Failed to Start: {self.calls_failed}")
                print(f"Timeouts: {self.calls_timeout}")
                print(f"Average Duration: {avg_duration:.1f}s")
                print(f"Success Rate: {success_rate:.1f}%")
                print(f"Immediate Hangup Rate: {immediate_rate:.1f}%")
                print(f"Selected Dispositions: {', '.join(self.selected_folders)}")
                print(f"Batch Size: {self.batch_size}")
                print("=" * 45)

    def signal_handler(self, signum, frame):
        print("\nReceived interrupt signal. Stopping test...")
        self.stop_event.set()

    def run_test(self):
        print("=" * 60)
        print("🎯 PYTHON HIGH-PERFORMANCE LOAD TEST WITH VICIDIAL HEADERS")
        print(f"Target: {self.concurrent_calls} concurrent calls for {self.test_duration // 60} minutes")
        print(f"SBC: {self.sbc_address}")
        print(f"Extension: {self.target_extension}")
        print(f"Client-Id: {self.client_id}")
        print(f"Long Calls Percent: {self.long_call_percent}%")
        print(f"Audio Base Path: {self.audio_base_path}")
        print(f"Selected Folders: {', '.join(self.selected_folders)}")
        print(f"Total Audio Files: {len(self.audio_files)}")
        print(f"Batch Size: {self.batch_size}")
        print(f"Started: {datetime.now()}")
        print("=" * 60)

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        generator_thread = threading.Thread(target=self.call_generator, daemon=True)
        reporter_thread = threading.Thread(target=self.status_reporter, daemon=True)

        generator_thread.start()
        reporter_thread.start()

        try:
            generator_thread.join()
            self.stop_event.set()

            print("✅ Test completed. Waiting 60 seconds for calls to finish...")
            time.sleep(60)

        except KeyboardInterrupt:
            print("\n❌ Test interrupted!")
            self.stop_event.set()

        self.generate_final_report()

    def generate_final_report(self):
        final_active = self.get_active_calls_count()
        avg_duration = sum(self.call_durations) / len(self.call_durations) if self.call_durations else 0

        print("\n" + "=" * 60)
        print("📊 FINAL COMPREHENSIVE REPORT")
        print("=" * 60)
        print(f"Test Configuration:")
        print(f"  - Target Concurrent: {self.concurrent_calls}")
        print(f"  - Duration: {self.test_duration // 60} minutes")
        print(f"  - SBC: {self.sbc_address}")
        print(f"  - Client-Id: {self.client_id}")
        print(f"  - Selected Folders: {', '.join(self.selected_folders)}")
        print(f"  - Audio Files Used: {len(self.audio_files)}")
        print(f"  - Batch Size: {self.batch_size}")
        print()
        print(f"Call Statistics:")
        print(f"  - Total Started: {self.calls_started}")
        print(f"  - Completed Normally: {self.calls_completed}")
        print(f"  - Immediate Hangups: {self.calls_immediate}")
        print(f"  - Failed to Start: {self.calls_failed}")
        print(f"  - Timeouts: {self.calls_timeout}")
        print(f"  - Still Active: {final_active}")
        print()

        if self.calls_started > 0:
            success_rate = (self.calls_completed * 100) / self.calls_started
            immediate_rate = (self.calls_immediate * 100) / self.calls_started
            failure_rate = (self.calls_failed * 100) / self.calls_started

            print(f"Success Rates:")
            print(f"  - Normal Completion: {success_rate:.1f}%")
            print(f"  - Immediate Hangup: {immediate_rate:.1f}%")
            print(f"  - Failed to Start: {failure_rate:.1f}%")
            print()

        print(f"Performance Metrics:")
        print(f"  - Average Call Duration: {avg_duration:.1f}s")
        print(f"  - Calls per Minute: {(self.calls_started * 60) // self.test_duration}")
        print()

        print(f"Analysis:")
        if self.calls_immediate > (self.calls_started * 0.25):
            print(f"⚠️  HIGH IMMEDIATE HANGUP RATE ({immediate_rate:.1f}%)")
            print(f"   - Possible SBC capacity limit reached")
            print(f"   - Authentication or connectivity issues")

        if self.calls_failed > (self.calls_started * 0.1):
            print(f"⚠️  HIGH FAILURE RATE ({failure_rate:.1f}%)")
            print(f"   - Check FreeSWITCH connectivity")

        calls_per_min = (self.calls_started * 60) // self.test_duration
        expected_calls_per_min = (self.concurrent_calls * 60) // 35  # Assuming 35s avg call

        if calls_per_min >= expected_calls_per_min * 0.8:
            print(f"✅ Load test successful - achieving expected call volume")
        else:
            print(f"⚠️  Lower than expected call volume - system may be at capacity")

        print()
        print(f"VICIdial Headers Used:")
        print(f"  - X-VICIdial-User-Agent: VICIdial-SIPP")
        print(f"  - X-VICIdial-Client-Id: {self.client_id}")
        print(f"  - X-VICIdial-Campaign-Id: camp01")
        print(f"  - X-VICIdial-User-Id: agent001")
        print()

        # Show disposition breakdown for selected folders only
        print(f"Selected Disposition Breakdown:")
        disposition_counts = {}
        for audio_file in self.audio_files:
            dispo = self.file_dispo_map[audio_file]
            disposition_counts[dispo] = disposition_counts.get(dispo, 0) + 1

        for dispo, count in sorted(disposition_counts.items()):
            print(f"  - {dispo}: {count} files")
        print()

        print(f"Files Generated:")
        print(f"  - Statistics: {self.stats_file}")
        print(f"  - Call Details: {self.call_log_file}")
        print(f"  - Header Details: {self.headers_log_file}")
        print()
        print(f"Completed: {datetime.now()}")
        print("=" * 60)


def show_folder_menu(available_folders):
    """Display interactive folder selection menu."""
    print("\n" + "=" * 50)
    print("📁 AVAILABLE DISPOSITION FOLDERS")
    print("=" * 50)

    for i, folder in enumerate(available_folders, 1):
        print(f"  {i}. {folder}")

    print(f"  {len(available_folders) + 1}. ALL FOLDERS")
    print("=" * 50)

    while True:
        try:
            choice = input("\nSelect folders (comma-separated numbers, e.g., 1,3,5 or 'all'): ").strip().lower()

            if choice in ['all', str(len(available_folders) + 1)]:
                return available_folders

            # Parse comma-separated choices
            selected_indices = [int(x.strip()) for x in choice.split(',')]
            selected_folders = []

            for idx in selected_indices:
                if 1 <= idx <= len(available_folders):
                    selected_folders.append(available_folders[idx - 1])
                else:
                    raise ValueError(f"Invalid choice: {idx}")

            if not selected_folders:
                raise ValueError("No folders selected")

            return selected_folders

        except (ValueError, IndexError) as e:
            print(f"❌ Invalid selection: {e}")
            print("Please enter numbers separated by commas, or 'all' for all folders")


def get_batch_size():
    """Get custom batch size from user."""
    while True:
        try:
            batch_size = int(input("Enter batch size (calls per batch, e.g., 25): "))
            if batch_size > 0:
                return batch_size
            else:
                print("Please enter a positive number.")
        except ValueError:
            print("Invalid input. Please enter a valid number.")


def get_campaign_audio_path(campaign_name):
    """Map campaign name to audio folder path.

    Matches ENUM_CAMPAIGN_TYPES from portal:
    MEDICARE = 'Medicare'
    ACA = 'Aca'
    FE = 'FE'
    SOLAR = 'Solar'
    HOME_IMPROVEMENT = 'Home Improvement'
    HOME_WARRANTY = 'Home Warranty'
    MED_ALERT = 'MedAlert'
    MVA = 'MVA'
    """
    campaign_mapping = {
        # Exact enum value matches (case-insensitive)
        'medicare': '/usr/share/freeswitch/sounds/happy_test_medicare/',
        'aca': '/usr/share/freeswitch/sounds/happy_test_aca/',
        'fe': '/usr/share/freeswitch/sounds/happy_test_fe/',
        'solar': '/usr/share/freeswitch/sounds/happy_test_solar/',
        'home improvement': '/usr/share/freeswitch/sounds/happy_test_hi/',
        'home warranty': '/usr/share/freeswitch/sounds/happy_test_hw/',
        'medalert': '/usr/share/freeswitch/sounds/happy_test_medalert/',
        'mva': '/usr/share/freeswitch/sounds/happy_test_acident/',  # MVA = Motor Vehicle Accident
        # Backward compatibility
        'accident': '/usr/share/freeswitch/sounds/happy_test_acident/'
    }

    return campaign_mapping.get(campaign_name.lower())


def main():
    parser = argparse.ArgumentParser(
        description='FreeSWITCH High-Performance Load Tester with Interactive Folder Selection')
    parser.add_argument('-c', '--concurrent', type=int, default=50, help='Concurrent calls (default: 50)')
    parser.add_argument('-d', '--duration', type=int, default=600, help='Test duration in seconds (default: 600)')
    parser.add_argument('-s', '--sbc', default='sbcserver12.idrakai.com', help='SBC address')
    parser.add_argument('-e', '--extension', default='911', help='Target extension')
    parser.add_argument('-a', '--audio-path', default='/usr/share/freeswitch/sounds/happy_test/',
                        help='Base path for audio files (default: /usr/share/freeswitch/sounds/happy_test/)')
    parser.add_argument('--client-id', default='roundrobin',
                        help='X-VICIdial-Client-Id value (e.g., roundrobin, leastconnect)')

    args = parser.parse_args()

    print("=" * 60)
    print("🎯 FreeSWITCH Load Tester with Interactive Menu")
    print("=" * 60)

    try:
        # First, discover available folders using static method
        available_folders = FreeSWITCHLoadTester.discover_folders_static(args.audio_path)

        # Show folder selection menu
        selected_folders = show_folder_menu(available_folders)
        print(f"\n✅ Selected folders: {', '.join(selected_folders)}")

        # Get batch size
        batch_size = get_batch_size()
        print(f"✅ Batch size: {batch_size}")

        # Get long calls percentage
        while True:
            try:
                long_calls_percent = float(input("Enter percentage of long calls (0-100): "))
                if 0 <= long_calls_percent <= 100:
                    break
                else:
                    print("Please enter a number between 0 and 100.")
            except ValueError:
                print("Invalid input. Please enter a valid number.")

        print(f"\n🔍 Initializing test with selected dispositions...")

        # Create the actual tester with selected folders
        tester = FreeSWITCHLoadTester(
            concurrent_calls=args.concurrent,
            test_duration=args.duration,
            sbc_address=args.sbc,
            target_extension=args.extension,
            audio_base_path=args.audio_path,
            client_id=args.client_id,
            long_call_percent=long_calls_percent,
            selected_folders=selected_folders,
            batch_size=batch_size
        )

        print(f"\n✅ Initialization complete! Ready to start load test.")
        print(f"📋 Test Summary:")
        print(f"   - Concurrent Calls: {args.concurrent}")
        print(f"   - Duration: {args.duration // 60} minutes")
        print(f"   - Selected Dispositions: {', '.join(selected_folders)}")
        print(f"   - Batch Size: {batch_size}")
        print(f"   - Long Calls: {long_calls_percent}%")
        print(f"   - Total Audio Files: {len(tester.audio_files)}")

        input("\nPress Enter to begin the test...")

        tester.run_test()

    except Exception as e:
        print(f"❌ Error initializing test: {e}")
        print("Please check:")
        print(f"  - Audio path exists: {args.audio_path}")
        print("  - Audio files are present in subdirectories")
        print("  - You have permission to read the files")
        print("  - FreeSWITCH is running and accessible")
        sys.exit(1)


if __name__ == "__main__":
