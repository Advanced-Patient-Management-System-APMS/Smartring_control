import asyncio, time, math, argparse, json
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from bleak import BleakClient, BleakScanner
import paho.mqtt.client as mqtt

# ======================
# MQTT 연결/전송 설정
# ======================
MQTT_HOST   = "100.112.74.119"
MQTT_PORT   = 1883
MQTT_USER   = "home-server"
MQTT_PASS   = "berry"
MQTT_TOPIC  = "AjouHospital/patient/7"  # 전송 토픽
MQTT_KEEPALIVE = 60
MQTT_QOS = 0
MQTT_RETAIN = False

# ======================
# BLE 장치/UUID 설정(Nordic UART)
# ======================
ADDRESS = "57:31:2C:2F:E8:C6"  # 링 MAC 주소 
UART_SERVICE_UUID = "6E40FFF0-B5A3-F393-E0A9-E50E24DCCA9E"
UART_RX_CHAR_UUID = "6E400002-B5A3-F393-E0A9-E50E24DCCA9E"  # Write
UART_TX_CHAR_UUID = "6E400003-B5A3-F393-E0A9-E50E24DCCA9E"  # Notify

# ======================
# 펌웨어와 주고받는 커맨드(Opcode) 정의
# ======================
CMD_START, CMD_STOP = 0x69, 0x6A  # CONTINUE 제거

def now_str():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

# 16바이트 고정 길이 패킷 생성(마지막 바이트는 체크섬)
# p[0] = op, p[1:1+len(payload)] = payload, p[15] = sum(p[0:15]) & 0xFF
def make_pkt(op: int, payload: bytes | None = None) -> bytearray:
    p = bytearray(16); p[0] = op & 0xFF
    if payload:
        if len(payload) > 14: raise ValueError("payload too long")
        p[1:1+len(payload)] = payload
    p[15] = sum(p[0:15]) & 0xFF
    return p

# HR/SpO2 측정 시작/정지 패킷 (펌웨어 규약에 맞춘 페이로드)
START_HR    = make_pkt(CMD_START,    b"\x01\x25")
STOP_HR     = make_pkt(CMD_STOP,     b"\x01\x00\x00")
START_SPO2  = make_pkt(CMD_START,    b"\x03\x25")
STOP_SPO2   = make_pkt(CMD_STOP,     b"\x03\x00\x00")

# ======================
# 동작 파라미터(윈도우/페이즈 길이 등)
# ======================
WINDOW_SEC              = 45.0    # 개별 측정 창 길이
PHASE_SEC               = 300.0   # SLEEP 모드에서 HR/SpO2 각 5분 페이즈
AWAKE_SPO2_PERIOD_MIN   = 5.0     # AWAKE 중 SpO2 창 주기(분)
START_SETTLE_SEC        = 0.4     # START 직후 안정화 대기
RESTART_GAP_SEC         = 0.25    # STOP→START 사이 간격
NO_FRAME_WATCHDOG       = 60.0    # 창 내부 프레임 미수신 시 재시작 기준

# 빈 창도 로그 남길지(디버깅용)
SUPPRESS_EMPTY_WINDOWS  = False

# ======================
# 수면 판정 파라미터(HR+RMSSD 단순 규칙)
# ======================
RHR_BASE            = 70.0
HR_SLEEP_FACTOR     = 0.93
HR_SLEEP_ABS        = 62.0
HR_WAKE_FACTOR      = 1.12
HR_WAKE_ABS         = 82.0
RMSSD_SLEEP_MIN     = 12.0
RMSSD_WAKE_MAX      = 8.0
SLEEP_ENTER_WINDOWS = 3
WAKE_ENTER_WINDOWS  = 2

# 원시 프레임 출력(디버깅)
PRINT_RAW = False

# ======================
# 유틸: 16바이트 프레임 단위 순회/파싱
# ======================
def chunks16(b: bytes):
    for i in range(0, len(b), 16):
        c = b[i:i+16]
        if len(c)==16: yield c

def csum_ok(c: bytes) -> bool:
    return (sum(c[0:15]) & 0xFF) == c[15]

def is_idle_spo2(c: bytes) -> bool:
    # 69 03 00..00 6c 패턴: 측정 값 없는 상태 프레임으로 간주
    return (len(c)==16 and c[0]==CMD_START and c[1]==0x03 and
            all(b==0 for b in c[2:15]) and ((c[0]+c[1]) & 0xFF)==c[15])

def parse_hr(c: bytes) -> int | None:
    if len(c)!=16 or c[0]!=CMD_START or not csum_ok(c): return None
    v = c[3];  return int(v) if 30 <= v <= 220 else None

def parse_spo2(c: bytes) -> int | None:
    if len(c)!=16 or c[0]!=CMD_START or not csum_ok(c) or is_idle_spo2(c): return None
    v = c[3];  return int(v) if 70 <= v <= 100 else None

def rmssd_est_guarded(hr_vals: list[int]) -> float:
    vals = [v for v in hr_vals if v and v>0]
    if len(vals) < 4 or len(set(vals)) < 3:
        return float('nan')
    ibi = [60000.0/v for v in vals]
    dif = [ibi[i+1]-ibi[i] for i in range(len(ibi)-1)]
    if not dif or all(abs(d) < 2.0 for d in dif):
        return float('nan')
    return math.sqrt(sum(d*d for d in dif)/len(dif))

# ======================
# 이벤트 데이터 구조
# ======================
@dataclass
class HREvent:
    t_start: datetime
    t_end: datetime
    hr_values: list[int]
    @property
    def mean(self) -> float:
        return sum(self.hr_values)/len(self.hr_values) if self.hr_values else float("nan")
    @property
    def rmssd(self) -> float:
        return rmssd_est_guarded(self.hr_values)

@dataclass
class SpO2Event:
    t: datetime
    spo2: int | None

# ======================
# 간단 수면 상태머신
# ======================
class SleepSM:
    def __init__(self):
        self.state = "AWAKE"
        self.hr_buf: deque[HREvent] = deque(maxlen=6)
        self._sleep_streak = 0
        self._wake_streak  = 0
    def on_hr(self, e: HREvent):
        self.hr_buf.append(e); self._tick()
    def on_spo2(self, _e: SpO2Event):
        pass
    def _tick(self):
        if not self.hr_buf: return
        last = self.hr_buf[-1]; hr_mean = last.mean
        if math.isnan(hr_mean): return
        rmvs = [ev.rmssd for ev in list(self.hr_buf)[-3:] if not math.isnan(ev.rmssd)]
        rmssd_med = (sorted(rmvs)[len(rmvs)//2] if rmvs else float('nan'))
        sleep_hr_hard = (hr_mean <= HR_SLEEP_ABS)
        sleep_hr_soft = (hr_mean <= RHR_BASE * HR_SLEEP_FACTOR)
        sleep_soft_plus_rm = (sleep_hr_soft and (not math.isnan(rmssd_med)) and (rmssd_med >= RMSSD_SLEEP_MIN))
        sleep_cond = sleep_hr_hard or sleep_soft_plus_rm
        wake_hr_abs  = (hr_mean >= HR_WAKE_ABS)
        wake_hr_rel  = (hr_mean >= RHR_BASE * HR_WAKE_FACTOR)
        wake_rm_low  = ((not math.isnan(rmssd_med)) and (rmssd_med <= RMSSD_WAKE_MAX))
        wake_cond = wake_hr_abs or wake_hr_rel or wake_rm_low
        if self.state == "AWAKE":
            self._sleep_streak = self._sleep_streak + 1 if sleep_cond else 0
            if self._sleep_streak >= SLEEP_ENTER_WINDOWS:
                prev = self.state; self.state = "SLEEP"
                self._sleep_streak = 0; self._wake_streak  = 0
                rm_txt = "N/A" if math.isnan(rmssd_med) else f"{rmssd_med:.1f}"
                print(f"[{now_str()}] [STATE] {prev} → {self.state} | hr_mean={hr_mean:.1f}, rmssd≈{rm_txt}")
        else:
            self._wake_streak = self._wake_streak + 1 if wake_cond else 0
            if self._wake_streak >= WAKE_ENTER_WINDOWS:
                prev = self.state; self.state = "AWAKE"
                self._wake_streak = 0; self._sleep_streak = 0
                rm_txt = "N/A" if math.isnan(rmssd_med) else f"{rmssd_med:.1f}"
                print(f"[{now_str()}] [STATE] {prev} → {self.state} | hr_mean={hr_mean:.1f}, rmssd≈{rm_txt}")

# ======================
# MQTT helper들
# ======================
_mqtt = None
_LAST_STATUS = None

def mqtt_connect():
    global _mqtt
    if _mqtt:
        return _mqtt
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
    client.loop_start()
    _mqtt = client
    return client

def mqtt_publish(timestamp: str, spo2_val, hr_val, status: str | None):
    global _LAST_STATUS
    client = mqtt_connect()
    payload = {
        "timestamp": timestamp,
        "spo2": spo2_val if spo2_val is not None else None,
        "heartrate": hr_val if hr_val is not None else None,
    }
    include_status = False
    if status is not None:
        if _LAST_STATUS is None or status != _LAST_STATUS:
            include_status = True
            _LAST_STATUS = status
    if include_status:
        payload["status"] = status
    try:
        client.publish(MQTT_TOPIC, json.dumps(payload), qos=MQTT_QOS, retain=MQTT_RETAIN)
    except Exception as e:
        print(f"[{now_str()}] [MQTT-ERR] {type(e).__name__}: {e}")

# ======================
# BLE helper들
# ======================
async def connect_ble(address: str, timeout: float = 60.0, attempts: int = 5):
    try:
        from bleak.backends.device import BLEAddressType
        addr_types = [BLEAddressType.PUBLIC, BLEAddressType.RANDOM]
    except Exception:
        addr_types = [None]
    backoff = 1.5
    last_err = None
    for attempt in range(1, attempts + 1):
        for at in addr_types:
            try:
                client = BleakClient(address, timeout=timeout, address_type=at) if at else BleakClient(address, timeout=timeout)
                await client.connect()
                print(f"[{now_str()}] connected (attempt {attempt}, addr_type={at})")
                return client
            except Exception as e:
                last_err = e
                print(f"[{now_str()}] [CONNECT] {type(e).__name__}: {e} (addr_type={at}, try {attempt}/{attempts})")
                await asyncio.sleep(backoff)
        backoff = min(backoff * 1.7, 8.0)
    print(f"[{now_str()}] [CONNECT] failed after {attempts} attempts: {last_err}")
    return None

async def safe_write(client: BleakClient, char, data, response=True):
    if not client.is_connected:
        return False
    try:
        await client.write_gatt_char(char, data, response=response)
        return True
    except Exception as e:
        print(f"[{now_str()}] [BLE-WRITE-ERR] {type(e).__name__}: {e}")
        return False

# 한 세션(연결~측정 루프~종료) 실행
async def run_session(address: str) -> bool:
    print(f"[{now_str()}] scan {address}")
    dev = await BleakScanner.find_device_by_address(address, timeout=20.0)
    if not dev:
        print("[!] device not found")
        return False

    sm = SleepSM()
    spo2_recent: deque[int] = deque(maxlen=40)

    last_frame_ts = time.monotonic()
    hr_vals_window: list[int] = []
    spo2_vals_window: list[int] = []

    collecting = {"hr": False, "spo2": False}
    stop_evt = asyncio.Event()
    disco_evt = asyncio.Event()

    AWAKE_SPO2_PERIOD_SEC = AWAKE_SPO2_PERIOD_MIN * 60.0
    last_awake_spo2_ts = time.monotonic()

    def emit_live_now(ts: datetime | None = None, status: str | None = None):
        vals = [ev.mean for ev in sm.hr_buf if not math.isnan(ev.mean)]
        hrm  = (sum(vals)/len(vals)) if vals else float('nan')
        rmvs = [ev.rmssd for ev in sm.hr_buf if not math.isnan(ev.rmssd)]
        rmed = (sorted(rmvs)[len(rmvs)//2] if rmvs else float('nan'))
        spo2_txt = f"min={min(spo2_recent)} max={max(spo2_recent)}" if len(spo2_recent)>0 else "n/a"
        hr_txt = f"{hrm:.1f}" if not math.isnan(hrm) else "N/A"
        rm_txt = f"{rmed:.1f}" if not math.isnan(rmed) else "N/A"
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else now_str()
        if status is None:
            status = "sleeping" if sm.state == "SLEEP" else "awaken"
        print(f"[{ts_str}] [LIVE] status={status} hr_mean={hr_txt} rmssd≈{rm_txt} spo2={spo2_txt}")

    def on_notify(_s, data: bytearray):
        nonlocal last_frame_ts, hr_vals_window, spo2_vals_window
        last_frame_ts = time.monotonic()
        for c in chunks16(bytes(data)):
            if not csum_ok(c) or c[0]!=CMD_START:
                continue
            if c[1]==0x01 and collecting["hr"]:
                v = parse_hr(c)
                if v is not None:
                    if PRINT_RAW: print(f"[{now_str()}] HR={v}")
                    hr_vals_window.append(v)
            elif c[1]==0x03 and collecting["spo2"]:
                if is_idle_spo2(c):
                    continue
                v = parse_spo2(c)
                if v is not None:
                    if PRINT_RAW: print(f"[{now_str()}] SpO2={v}%")
                    spo2_vals_window.append(v)
                    spo2_recent.append(v)

    def _on_disconnect(_c):
        print(f"[{now_str()}] [BLE] disconnected (callback)")
        try:
            disco_evt.set()
            stop_evt.set()
        except Exception:
            pass

    mqtt_connect()

    async with BleakClient(address, timeout=60.0) as client:
        client.set_disconnected_callback(_on_disconnect)
        print(f"[{now_str()}] connected (KEEP-ALIVE disabled)")
        if not client.services:
            await client.get_services()
        svc = client.services.get_service(UART_SERVICE_UUID)
        if not svc:
            print("[!] UART service not found")
            return False
        rx = svc.get_characteristic(UART_RX_CHAR_UUID)
        tx = svc.get_characteristic(UART_TX_CHAR_UUID)
        if not rx or not tx:
            print("[!] RX/TX characteristic not found")
            return False

        await client.start_notify(tx, on_notify)
        ble_lock = asyncio.Lock()  # START/STOP 충돌 방지용

        # ------------------
        # HR 창 수행 루틴
        # ------------------
        async def do_hr_window():
            """HR 45s 창을 수행하고 요약 로그 및 MQTT(HR-only) 전송.
            데이터가 없으면 No_wear 상태로 전송한다."""
            nonlocal hr_vals_window, last_frame_ts
            hr_vals_window = []
            collecting["hr"] = True
            collecting["spo2"] = False

            t0 = datetime.now()
            async with ble_lock:
                await safe_write(client, rx, START_HR, response=True)
            await asyncio.sleep(START_SETTLE_SEC)

            end_t = time.monotonic() + WINDOW_SEC
            while (
                time.monotonic() < end_t
                and client.is_connected
                and not disco_evt.is_set()
                and not stop_evt.is_set()
            ):
                if NO_FRAME_WATCHDOG and (time.monotonic() - last_frame_ts > NO_FRAME_WATCHDOG):
                    print(f"[{now_str()}] [WATCHDOG] HR restart")
                    async with ble_lock:
                        await safe_write(client, rx, STOP_HR, response=True)
                        await asyncio.sleep(RESTART_GAP_SEC)
                        await safe_write(client, rx, START_HR, response=True)
                    await asyncio.sleep(START_SETTLE_SEC)
                await asyncio.sleep(0.1)

            async with ble_lock:
                await safe_write(client, rx, STOP_HR, response=True)
            collecting["hr"] = False

            t1 = datetime.now()
            ev = HREvent(t_start=t0, t_end=t1, hr_values=hr_vals_window[:])

            if len(ev.hr_values) == 0:
                print(f"[{now_str()}] [HR_WIN] n=0 → No_wear (no valid HR frames)")
                emit_live_now(ts=t1, status="No_wear")
                mqtt_publish(
                    t1.strftime("%Y-%m-%d %H:%M:%S"),
                    spo2_val=None,
                    hr_val=None,
                    status="No_wear",
                )
                return

            mean_txt = f"{ev.mean:.1f}" if not math.isnan(ev.mean) else "N/A"
            rm = ev.rmssd
            rm_txt = f"{rm:.1f}" if not math.isnan(rm) else "N/A"
            if (not SUPPRESS_EMPTY_WINDOWS):
                print(f"[{now_str()}] [HR_WIN] n={len(ev.hr_values)} mean={mean_txt} rmssd≈{rm_txt}")

            sm.on_hr(ev)
            status_str = "sleeping" if sm.state == "SLEEP" else "awaken"

            emit_live_now(ts=t1, status=status_str)
            mqtt_publish(
                t1.strftime("%Y-%m-%d %H:%M:%S"),
                spo2_val=None,
                hr_val=int(round(ev.mean)),
                status=status_str,
            )

        # ------------------
        # SpO2 창 수행 루틴
        # ------------------
        async def do_spo2_window():
            """SpO2 45s 창 수행. 시작 전 HR을 명시적으로 STOP하여 겹침 방지.
            유효 샘플이 없으면 No_wear를 전송한다."""
            nonlocal spo2_vals_window, last_frame_ts
            async with ble_lock:
                await safe_write(client, rx, STOP_HR, response=True)
                await asyncio.sleep(RESTART_GAP_SEC)

            spo2_vals_window = []
            collecting["hr"] = False
            collecting["spo2"] = True

            t0 = datetime.now()
            async with ble_lock:
                ok = await safe_write(client, rx, START_SPO2, response=True)
                if not ok:
                    collecting["spo2"] = False
                    if not SUPPRESS_EMPTY_WINDOWS:
                        print(f"[{now_str()}] [SPO2_WIN] n=0 (start_fail) → No_wear")
                    emit_live_now(ts=t0, status="No_wear")
                    mqtt_publish(
                        t0.strftime("%Y-%m-%d %H:%M:%S"),
                        spo2_val=None,
                        hr_val=None,
                        status="No_wear",
                    )
                    return
            await asyncio.sleep(START_SETTLE_SEC)

            end_t = time.monotonic() + WINDOW_SEC
            while (
                time.monotonic() < end_t
                and client.is_connected
                and not disco_evt.is_set()
                and not stop_evt.is_set()
            ):
                if NO_FRAME_WATCHDOG and (time.monotonic() - last_frame_ts > NO_FRAME_WATCHDOG):
                    print(f"[{now_str()}] [WATCHDOG] SpO2 restart")
                    async with ble_lock:
                        await safe_write(client, rx, STOP_SPO2, response=True)
                        await asyncio.sleep(RESTART_GAP_SEC)
                        await safe_write(client, rx, START_SPO2, response=True)
                    await asyncio.sleep(START_SETTLE_SEC)
                await asyncio.sleep(0.1)

            async with ble_lock:
                await safe_write(client, rx, STOP_SPO2, response=True)
            collecting["spo2"] = False

            t1 = datetime.now()
            if spo2_vals_window:
                s = sorted(spo2_vals_window)
                mid = len(s) // 2
                spo2_rep = s[mid] if len(s) % 2 == 1 else int(round((s[mid - 1] + s[mid]) / 2))

                if (not SUPPRESS_EMPTY_WINDOWS):
                    print(
                        f"[{now_str()}] [SPO2_WIN] n={len(spo2_vals_window)} "
                        f"min={min(spo2_vals_window)} max={max(spo2_vals_window)} median={spo2_rep}"
                    )

                status_str = "sleeping" if sm.state == "SLEEP" else "awaken"
                emit_live_now(ts=t1, status=status_str)
                mqtt_publish(
                    t1.strftime("%Y-%m-%d %H:%M:%S"),
                    spo2_val=int(spo2_rep),
                    hr_val=None,
                    status=status_str,
                )
            else:
                if (not SUPPRESS_EMPTY_WINDOWS):
                    print(f"[{now_str()}] [SPO2_WIN] n=0 (no valid frames) → No_wear")
                emit_live_now(ts=t1, status="No_wear")
                mqtt_publish(
                    t1.strftime("%Y-%m-%d %H:%M:%S"),
                    spo2_val=None,
                    hr_val=None,
                    status="No_wear",
                )

        # ------------------ 세션 메인 루프 ------------------
        try:
            while client.is_connected and not disco_evt.is_set():
                if sm.state == "AWAKE":
                    if time.monotonic() - last_awake_spo2_ts >= AWAKE_SPO2_PERIOD_SEC:
                        await do_hr_window()
                        if disco_evt.is_set() or stop_evt.is_set(): break
                        await do_spo2_window()
                        last_awake_spo2_ts = time.monotonic()
                    else:
                        await do_hr_window()
                        if disco_evt.is_set() or stop_evt.is_set(): break
                else:
                    phase_start = time.monotonic()
                    print(f"[{now_str()}] [ENTER] HR phase (5m)")
                    while (time.monotonic() - phase_start < PHASE_SEC and
                           sm.state == "SLEEP" and client.is_connected and
                           not disco_evt.is_set() and not stop_evt.is_set()):
                        await do_hr_window()
                    if sm.state != "SLEEP" or disco_evt.is_set() or stop_evt.is_set():
                        continue
                    phase_start = time.monotonic()
                    print(f"[{now_str()}] [ENTER] SPO2 phase (5m)")
                    while (time.monotonic() - phase_start < PHASE_SEC and
                           sm.state == "SLEEP" and client.is_connected and
                           not disco_evt.is_set() and not stop_evt.is_set()):
                        await do_spo2_window()

        except KeyboardInterrupt:
            print("\n[Exit]")
        finally:
            # 종료/정리: 측정 중지, notify 해제, MQTT 정리
            try:
                await safe_write(client, rx, STOP_HR,  response=True)
                await safe_write(client, rx, STOP_SPO2, response=True)
            except Exception:
                pass
            try:
                if client.is_connected:
                    await client.stop_notify(tx)
            except Exception:
                pass
            try:
                c = mqtt_connect()
                c.loop_stop(); c.disconnect()
            except Exception:
                pass
            print(f"[{now_str()}] disconnected")

    return True

# ======================
# 메인 루프: 세션 실패 시 지수 백오프 재시도
# ======================
async def main_loop(address: str):
    BASE_BACKOFF = 2.0
    MAX_BACKOFF  = 30.0
    SUCCESS_RESET_SEC = 90.0

    backoff = BASE_BACKOFF

    while True:
        t0 = time.monotonic()
        try:
            ok = await run_session(address)
        except Exception as e:
            print(f"[{now_str()}] [MAIN] run_session raised: {type(e).__name__}: {e}")
        dt = time.monotonic() - t0

        if dt >= SUCCESS_RESET_SEC:
            backoff = BASE_BACKOFF
        else:
            import random
            backoff = min(MAX_BACKOFF, BASE_BACKOFF + random.random() * (backoff * 3 - BASE_BACKOFF))

        print(f"[{now_str()}] [RECONNECT] retry in {backoff:.1f}s (last session {dt:.1f}s)")
        try:
            await asyncio.sleep(backoff)
        except asyncio.CancelledError:
            return

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-a","--address", default=ADDRESS)
    args = ap.parse_args()
    try:
        asyncio.run(main_loop(args.address))
    except KeyboardInterrupt:
        print("\n[Exit]")
