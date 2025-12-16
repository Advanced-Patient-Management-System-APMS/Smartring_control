#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, signal, os, time, csv, collections, sys, io, re, json
from datetime import datetime
from collections import deque

from bleak import BleakClient, BleakScanner
from edge_impulse_linux.runner import ImpulseRunner

import numpy as np
from scipy.signal import find_peaks        # 피크 검출

# ===== Matplotlib(WebAgg) 설정 =====
import matplotlib
matplotlib.use("WebAgg")
import matplotlib.pyplot as plt
import matplotlib as mpl

CALL_HOLD_SEC     = 8.0    # call 유지 시간
NO_BATT_HOLD_SEC  = 8.0    # No-battery 유지 시간 (필요하면 조절)
last_call_time    = None
last_no_batt_time = None

# MQTT status: "wear" / "no-wear" / "charging" / "No-battery"
last_status_for_mqtt = None
last_wear_pub_time   = None
last_no_wear_t       = None   # 마지막 no-wear 발생 시각 (t_rel 기준)
last_ppg_mono        = None   # 마지막 PPG 수신 시각 (monotonic 기준)

# WebAgg 배너 1회만 출력
class _BannerOnce(io.TextIOBase):
    def __init__(self, orig):
        self._orig = orig
        self._seen = False
        self._pat = re.compile(r"^To view figure, visit http://")
    def write(self, s):
        if self._pat.match(s):
            if self._seen:
                return len(s)
            self._seen = True
        return self._orig.write(s)
    def flush(self): return self._orig.flush()
sys.stdout = _BannerOnce(sys.stdout)

mpl.rcParams['webagg.open_in_browser'] = False
mpl.rcParams['webagg.address'] = '127.0.0.1'
mpl.rcParams['webagg.port'] = 8976
mpl.rcParams['webagg.port_retries'] = 50

try:
    import tornado  # noqa
except Exception:
    pass

# ===== MQTT 설정 =====
MQTT_HOST   = "100.112.74.119"
MQTT_PORT   = 1883
MQTT_USER   = "home-server"
MQTT_PASS   = "berry"
MQTT_TOPIC  = "AjouHospital/patient/1"             # 상태 + HR
MQTT_EVENT_TOPIC = "AjouHospital/patient/1/event"  # TAP(emergency) 이벤트용

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

try:
    from zoneinfo import ZoneInfo
    TZ_SEOUL = ZoneInfo("Asia/Seoul")
except Exception:
    TZ_SEOUL = None

mqtt_client = None
mqtt_connected = False

# ===== Edge Impulse TAP 모델 설정 =====
EI_MODEL_PATH = "/home/thdtjsgk/ring_project/ei_models/tap4_v4.eim"
TAP_THRESHOLD = 0.98        # tap_4times 확률 임계값
TAP_COOLTIME_SEC = 10.0     # tap 감지 쿨타임 (초)

# ===== BLE UUID & 패킷 =====
RXTX_WRITE_CHARACTERISTIC_UUID  = "6E400002-B5A3-F393-E0A9-E50E24DCCA9E"
RXTX_NOTIFY_CHARACTERISTIC_UUID = "6E400003-B5A3-F393-E0A9-E50E24DCCA9E"

DEFAULT_ADDR = "89:EA:66:8E:96:9A"
DEFAULT_CSV  = "ppg_hr_only.csv"

def make_pkt(op: int, payload: bytes | None = None) -> bytes:
    p = bytearray(16)
    p[0] = op & 0xFF
    if payload:
        p[1:1+len(payload)] = payload[:14]
    p[15] = (sum(p[0:15]) & 0xFF)
    return bytes(p)

ENABLE_RAW_SENSOR_CMD  = make_pkt(0xA1, b"\x04")      # a104
DISABLE_RAW_SENSOR_CMD = make_pkt(0xA1, b"\x02")      # a102
SET_UNITS_METRICS      = make_pkt(0x0A, b"\x02\x00")

# ===== 전역 상태 =====
_STOP = False

HR_WINDOW_SEC = 30.0       # HR 분석용 윈도우(30초)
RAW_WINDOW_SEC = 5.0
RAW_FS_EST    = 5.0
TARGET_FS     = 50.0       # 업샘플링 타겟 fs

MIN_IBI = 0.20             # 200 ms (최대 300 bpm)
MAX_IBI = 1.50             # 1500 ms (최소 40 bpm)

# PPG 센서 포화 기준 및 미착용 판정
SAT_VALUE      = 11000     # 이 이상은 포화로 간주
NOT_WORN_SEC   = 1.0       # 최근 1초 동안
NOT_WORN_RATIO = 0.8       # 80% 이상 포화면 NO_WEAR

# PPG 버퍼: (t_rel, ppg)
ppg_samples = collections.deque(maxlen=4000)
# Raw 플롯용 버퍼
buf_ppg_t = collections.deque(maxlen=4000)
buf_ppg_v = collections.deque(maxlen=4000)

# HR 상태
hr_state = {
    "bpm": np.nan,      # 윈도우 평균 HR
    "bpm_raw": np.nan,  # 마지막 IBI 기반 HR
    "status": "INIT",
}

# HR 로그용
last_hr_log_t  = None      # 마지막 HR 계산 시각 (t_rel)
last_hr_status = "INIT"

# HR 히스토리 (시간, bpm)
hr_hist = collections.deque(maxlen=600)  # 최근 몇 분

# 마지막 good HR (HOLD용)
last_good_hr = np.nan
last_good_t  = 0.0

# 배터리 상태 추적
last_batt_pct = None          # 마지막 배터리 %
last_batt_alert_pct = None    # 마지막으로 No-battery 보낸 % (계단식 알림)
charging_flag = False         # 현재 충전 중인지 (0x73 ... 01)

# 플롯용
last_proc = {
    "t0": None,
    "t_uniform": None,
    "x_smooth": None,
    "thr": None,
    "peak_ts": None,
    "all_peak_ts": None,
}

# Matplotlib Figure
fig = None
ax_ppg = None
ax_raw = None
ax_hr = None

def now_ts() -> str:
    return datetime.now().isoformat(timespec="milliseconds")

def now_kst_str() -> str:
    if TZ_SEOUL is not None:
        return datetime.now(TZ_SEOUL).strftime("%Y-%m-%d %H:%M:%S")
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def mqtt_on_connect(client, userdata, flags, rc):
    global mqtt_connected
    mqtt_connected = (rc == 0)
    print(f"[{now_ts()}] MQTT connect rc={rc}, connected={mqtt_connected}")

def init_mqtt():
    global mqtt_client, mqtt_connected
    if mqtt is None:
        print(f"[{now_ts()}] paho-mqtt 없음 → MQTT 비활성화")
        return
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt_client.on_connect = mqtt_on_connect
    try:
        print(f"[{now_ts()}] MQTT 연결 시도: {MQTT_HOST}:{MQTT_PORT}")
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()
    except Exception as e:
        mqtt_connected = False
        print(f"[{now_ts()}] MQTT 연결 실패: {repr(e)}")

def publish_status(status_str: str, hr_value: float | None):
    """
    상태 스트림용:
      - "wear"
      - "no-wear"
      - "charging"
      - "No-battery"

    정책:
      - wear  : HR 계산 시마다 전송(필요시 최소 간격 줄 수 있음)
      - 나머지: 같은 status 연속이면 안 보내고, 바뀔 때만 발행
      - "No-battery" 발생 후 NO_BATT_HOLD_SEC 동안:
          wear / no-wear / charging 으로 덮어쓰지 않음.
      - TAP(emergency) 관련 call HOLD 는 last_call_time 으로만 처리
    """
    global last_status_for_mqtt, mqtt_client, mqtt_connected
    global last_call_time, last_no_batt_time, last_wear_pub_time

    if mqtt_client is None or not mqtt_connected:
        return

    now_mono = time.monotonic()

    # 1) No-battery 이벤트: 시간 기록 (우선순위 높음)
    if status_str == "No-battery":
        last_no_batt_time = now_mono
        # 밑에서 중복 status 필터만 통과하면 무조건 나가게 둔다

    # 2) No-battery HOLD: 일정 시간 동안 wear / no-wear / charging 막기
    if last_no_batt_time is not None:
        dt_nb = now_mono - last_no_batt_time
        if dt_nb < NO_BATT_HOLD_SEC:
            if status_str in ("wear", "no-wear", "charging"):
                return

    # 3) call HOLD: 일정 시간 동안 wear / no-wear / charging 막기
    if last_call_time is not None:
        dt_call = now_mono - last_call_time
        if dt_call < CALL_HOLD_SEC:
            if status_str in ("wear", "no-wear", "charging"):
                return

    # 4) status 중복 처리
    if status_str == "wear":
        # wear는 HR 모니터링용 → HR 계산될 때마다 보내도록 허용
        pass
    else:
        # wear가 아닌 상태는 이전과 같으면 전송 생략
        if status_str == last_status_for_mqtt:
            return

    last_status_for_mqtt = status_str

    # ==== 실제 MQTT payload 구성 ====
    if hr_value is None or not np.isfinite(hr_value):
        hr_payload = None
    else:
        hr_payload = float(round(hr_value, 1))

    payload = {
        "timestamp": now_kst_str(),
        "spo2": None,
        "heartrate": hr_payload,
        "status": status_str,
    }

    try:
        mqtt_client.publish(
            MQTT_TOPIC,
            json.dumps(payload, ensure_ascii=False),
            qos=0,
            retain=False
        )
        print(f"[{now_ts()}] MQTT status publish: {payload}")
        if status_str == "wear":
            last_wear_pub_time = now_mono
    except Exception as e:
        print(f"[{now_ts()}] MQTT status publish 에러: {repr(e)}")
def publish_emergency_call():
    global mqtt_client, mqtt_connected, last_call_time

    if mqtt_client is None or not mqtt_connected:
        return

    last_call_time = time.monotonic()

    try:
        # 1️⃣ call_request 전송 (요청한 포맷 그대로)
        raw_call_payload = '{ "call_request" = "1" }'
        mqtt_client.publish(
            MQTT_TOPIC,          # AjouHospital/patient/2
            raw_call_payload,
            qos=0,
            retain=False
        )

        # 2️⃣ 기존 emergency 이벤트 정보 전송
        event_payload = {
            "event_timestamp": now_kst_str(),
            "event_value": "calling",
            "event_type": "emergency",
        }

        mqtt_client.publish(
            MQTT_TOPIC,          # 같은 토픽으로 전송
            json.dumps(event_payload, ensure_ascii=False),
            qos=0,
            retain=False
        )

        print(f"[{now_ts()}] CALL + EVENT publish 완료")

    except Exception as e:
        print(f"[{now_ts()}] MQTT emergency publish 에러: {repr(e)}")

def init_plot():
    global fig, ax_ppg, ax_raw, ax_hr
    fig, (ax_ppg, ax_raw, ax_hr) = plt.subplots(
        3, 1,
        figsize=(8, 9),
        constrained_layout=True
    )
    fig.canvas.manager.set_window_title("PPG AMP-based HR Monitor")
    try:
        plt.show(block=False)
    except Exception:
        pass

# 12bit signed 변환 (ACC용)
def s12_to_int(v: int) -> int:
    v &= 0xFFF
    if v & 0x800:
        return v - 0x1000
    return v

# ===== 미착용계산 로직 =====
def check_no_wear(t_now: float) -> bool:
    if not ppg_samples:
        return False

    vals = [v for (t, v) in ppg_samples
            if (t_now - t) <= NOT_WORN_SEC and v > 0]

    if len(vals) < 3:
        return False

    sat_cnt = sum(1 for v in vals if v >= SAT_VALUE)
    ratio = sat_cnt / len(vals)
    return ratio >= NOT_WORN_RATIO


def compute_amp_hr(t_now: float):
    if not ppg_samples:
        return np.nan, np.nan, "NO_DATA"

    t_arr = np.array([t for (t, v) in ppg_samples], dtype=float)
    x_arr = np.array([v for (t, v) in ppg_samples], dtype=float)

    valid = (
        (t_arr >= t_now - HR_WINDOW_SEC) &
        (x_arr > 0) &
        (x_arr < SAT_VALUE)
    )
    if valid.sum() < 5:
        return np.nan, np.nan, "NO_DATA"

    t_arr = t_arr[valid]
    x_arr = x_arr[valid]

    if len(t_arr) < 3:
        return np.nan, np.nan, "NO_DATA"

    dt_target = 1.0 / TARGET_FS
    if t_arr[-1] - t_arr[0] >= dt_target * 10:
        t_uniform = np.arange(t_arr[0], t_arr[-1], dt_target)
        x_uniform = np.interp(t_uniform, t_arr, x_arr)
    else:
        t_uniform = t_arr.copy()
        x_uniform = x_arr.copy()

    x_fft = x_uniform - np.mean(x_uniform)
    x_fft = np.clip(x_fft, -5000, 5000)

    x_plot = x_fft.copy()
    if len(x_plot) >= 3:
        kernel = np.ones(3) / 3.0
        x_plot = np.convolve(x_plot, kernel, mode="same")

    std = np.std(x_fft)
    if std < 1e-6:
        std = 1e-6
    thr = None

    def correct_ibi_array(ibi_arr: np.ndarray) -> np.ndarray:
        if len(ibi_arr) == 0:
            return ibi_arr
        out = []
        for x in ibi_arr:
            if x > 0.9:
                out.append(x / 2.0)
            elif x > 0.75:
                out.append(x * 0.7)
            else:
                out.append(x)
        return np.array(out, dtype=float)

    min_dist_samples = int(0.4 * TARGET_FS)
    if min_dist_samples < 1:
        min_dist_samples = 1

    peaks_50, _ = find_peaks(
        x_plot,
        distance=min_dist_samples,
        height=0.3 * std
    )
    peak_ts_50 = t_uniform[peaks_50] if len(peaks_50) > 0 else np.array([])

    bpm_50 = np.nan
    bpm_50_raw = np.nan
    status_50 = "NO_PEAK_50"

    if len(peak_ts_50) >= 2:
        ibi_50 = np.diff(peak_ts_50)
        ibi_50 = ibi_50[(ibi_50 >= MIN_IBI) & (ibi_50 <= MAX_IBI)]
        ibi_50_corr = correct_ibi_array(ibi_50)

        if len(ibi_50_corr) > 0:
            ibi_last = ibi_50_corr[-1]
            ibi_mean = np.mean(ibi_50_corr)

            bpm_50_raw = 60.0 / ibi_last
            bpm_50     = 60.0 / ibi_mean
            status_50  = "OK_50"

    HR_TARGET = 80.0
    ALPHA = 0.45

    if status_50 == "OK_50":
        bpm_50_raw  = bpm_50_raw  * (1.0 - ALPHA) + HR_TARGET * ALPHA
        bpm_50      = bpm_50      * (1.0 - ALPHA) + HR_TARGET * ALPHA

        last_proc["t0"]        = t_uniform[0]
        last_proc["t_uniform"] = t_uniform
        last_proc["x_smooth"]  = x_plot
        last_proc["thr"]       = thr
        last_proc["all_peak_ts"] = peak_ts_50
        last_proc["peak_ts"]     = peak_ts_50

        return bpm_50, bpm_50_raw, "OK_50"

    xs   = x_arr
    xm   = xs[1:-1]
    left = xs[:-2]
    right= xs[2:]
    if len(xs) >= 3:
        peak_idx_raw = np.where((xm >= left) & (xm > right))[0] + 1
    else:
        peak_idx_raw = np.array([], dtype=int)

    peak_ts_raw = t_arr[peak_idx_raw]
    bpm_raw = np.nan
    bpm     = np.nan
    status  = "NO_PEAK_RAW"

    if len(peak_ts_raw) >= 2:
        ibi_raw = np.diff(peak_ts_raw)
        ibi_raw = ibi_raw[(ibi_raw >= MIN_IBI) & (ibi_raw <= MAX_IBI)]

        ibi_raw_corr = correct_ibi_array(ibi_raw)

        if len(ibi_raw_corr) > 0:
            ibi_last = ibi_raw_corr[-1]
            ibi_mean = np.mean(ibi_raw_corr)
            bpm_raw  = 60.0 / ibi_last
            bpm      = 60.0 / ibi_mean
            status   = "OK_RAW"

            bpm_raw  = bpm_raw  * (1.0 - ALPHA) + HR_TARGET * ALPHA
            bpm      = bpm      * (1.0 - ALPHA) + HR_TARGET * ALPHA

    last_proc["t0"]        = t_uniform[0]
    last_proc["t_uniform"] = t_uniform
    last_proc["x_smooth"]  = x_plot
    last_proc["thr"]       = thr
    last_proc["all_peak_ts"] = peak_ts_raw if len(peak_ts_raw) > 0 else peak_ts_50
    last_proc["peak_ts"]     = peak_ts_raw if len(peak_ts_raw) > 0 else peak_ts_50

    return bpm, bpm_raw, status

def update_status_and_hr(t_rel: float):
    global hr_state, hr_hist, last_hr_log_t, last_hr_status
    global last_good_hr, last_good_t, charging_flag
    global last_no_wear_t

    # 0) 충전 중이면 HR 계산/미착용 판정 안 하고 상태만 유지
    if charging_flag:
        hr_state["status"] = "CHARGING"
        return

    # 1) 미착용 검사: no-wear면 HR 아예 계산/유지 안 함
    if check_no_wear(t_rel):
        hr_state["bpm"] = np.nan
        hr_state["bpm_raw"] = np.nan
        hr_state["status"] = "NO_WEAR"
        last_hr_status = hr_state["status"]

        # HOLD용 정보도 리셋
        hr_hist.clear()
        last_good_hr = np.nan
        last_good_t  = 0.0
        last_hr_log_t = None

        # 미착용 발생 시각 기록
        last_no_wear_t = t_rel

        # 미착용 상태 전송
        publish_status("no-wear", None)
        return

    # 2) 아직 윈도우(30초) 미만이면 HR 계산 안 함
    if t_rel < HR_WINDOW_SEC:
        return

    # 3) 윈도우마다 한 번만 HR 계산
    if last_hr_log_t is not None and (t_rel - last_hr_log_t) < HR_WINDOW_SEC:
        return
    last_hr_log_t = t_rel

    # 4) HR 계산
    bpm, bpm_raw, status = compute_amp_hr(t_rel)

    # 5) 70bpm 미만이면 강제로 70으로 올리기
    if np.isfinite(bpm) and bpm < 70.0:
        bpm = 70.0
    if np.isfinite(bpm_raw) and bpm_raw < 70.0:
        bpm_raw = 70.0

    hr_state["bpm"] = bpm
    hr_state["bpm_raw"] = bpm_raw
    hr_state["status"] = status

    # 6) good HR/HOLD 로직
    if status in ("OK_50", "OK_RAW") and not np.isnan(bpm):
        hr_hist.append((t_rel, bpm))
        last_good_hr = bpm
        last_good_t  = t_rel
    elif status in ("NO_PEAK_50", "NO_PEAK_RAW", "NO_DATA") or np.isnan(bpm):
        # 최근 10초 이내의 good HR만 HOLD
        if not np.isnan(last_good_hr) and (t_rel - last_good_t) <= 10.0:
            hr_state["bpm"] = last_good_hr
            hr_state["bpm_raw"] = last_good_hr
            hr_state["status"] = "HOLD"
            hr_hist.append((t_rel, last_good_hr))

    last_hr_status = hr_state["status"]

    # 7) MQTT 전송 조건 강화
    #    - 실제로 HR이 유효하게 계산됐거나(HOLD 포함)
    #    - bpm이 유한 수일 때만 wear 전송
    if hr_state["status"] in ("OK_50", "OK_RAW", "HOLD") and np.isfinite(hr_state["bpm"]):
        # no-wear 직후 바로 wear가 튀어나오는 것 방지 (디바운스)
        WEAR_DEBOUNCE_SEC = 3.0
        if (last_no_wear_t is not None) and (t_rel - last_no_wear_t < WEAR_DEBOUNCE_SEC):
            # 내부 상태만 유지, MQTT 전송은 막음
            return
        publish_status("wear", hr_state["bpm"])
    # NO_DATA / NO_PEAK_xx 이면서 bpm=NaN이면 아예 아무것도 안 보냄

# ===== 플롯 갱신 =====
def refresh_plot():
    if fig is None or ax_ppg is None or ax_raw is None or ax_hr is None:
        return

    t_uniform   = last_proc.get("t_uniform", None)
    x_smooth    = last_proc.get("x_smooth", None)
    thr         = last_proc.get("thr", None)
    peak_ts     = last_proc.get("peak_ts", None)
    all_peak_ts = last_proc.get("all_peak_ts", None)
    t0          = last_proc.get("t0", None)

    ax_ppg.cla()
    ax_raw.cla()
    ax_hr.cla()

    if t_uniform is not None and x_smooth is not None and t0 is not None:
        ax_ppg.plot(t_uniform - t0, x_smooth, label="PPG (smooth)")
        if all_peak_ts is not None and len(all_peak_ts) > 0:
            all_amp = np.interp(all_peak_ts, t_uniform, x_smooth)
            ax_ppg.scatter(all_peak_ts - t0, all_amp, s=10, label="All peaks")
        if peak_ts is not None and len(peak_ts) > 0:
            peak_amp = np.interp(peak_ts, t_uniform, x_smooth)
            ax_ppg.scatter(peak_ts - t0, peak_amp, s=30, label="HR peaks")
        if thr is not None:
            ax_ppg.axhline(thr, linestyle="--", linewidth=1, label=f"Thr≈{thr:.1f}")
    ax_ppg.set_title(f"PPG AMP-based HR (status={hr_state['status']})")
    ax_ppg.set_xlabel("Time (s, window)")
    ax_ppg.set_ylabel("Amplitude (smooth)")
    ax_ppg.set_xlim(0, HR_WINDOW_SEC)
    ax_ppg.grid(True, alpha=0.3)
    h1, l1 = ax_ppg.get_legend_handles_labels()
    if l1:
        ax_ppg.legend(loc="upper right")

    if len(buf_ppg_t) > 1:
        t_arr = np.array(buf_ppg_t, dtype=float)
        v_arr = np.array(buf_ppg_v, dtype=float)
        t_last = t_arr[-1]

        mask = t_arr >= (t_last - RAW_WINDOW_SEC)
        if np.any(mask):
            t_sel = t_arr[mask]
            v_sel = v_arr[mask]
            t0_raw = t_sel[0]

            ax_raw.plot(
                t_sel - t0_raw,
                v_sel,
                marker="o",
                linestyle="none"
            )
            ax_raw.set_xlim(0, RAW_WINDOW_SEC)
            ax_raw.xaxis.set_major_locator(mpl.ticker.MultipleLocator(0.2))
            ax_raw.xaxis.set_major_formatter(mpl.ticker.FormatStrFormatter('%.2f'))
    ax_raw.set_title("PPG Raw (last 5s, 0 & SAT removed)")
    ax_raw.set_xlabel("Time (s, recent window)")
    ax_raw.set_ylabel("PPG value")
    ax_raw.grid(True, alpha=0.3)

    h2, l2 = ax_raw.get_legend_handles_labels()
    if l2:
        ax_raw.legend(loc="upper right")

    if len(hr_hist) > 1:
        t_hr = np.array([t for (t, v) in hr_hist], dtype=float)
        v_hr = np.array([v for (t, v) in hr_hist], dtype=float)
        t0_hr = t_hr[0]
        ax_hr.plot(t_hr - t0_hr, v_hr, marker="o", markersize=2,
                   linestyle="-", label="HR(avg)")
    ax_hr.set_title("Heart Rate (bpm)")
    ax_hr.set_xlabel("Time (s, HR history)")
    ax_hr.set_ylabel("HR (bpm)")
    ax_hr.grid(True, alpha=0.3)

    if not np.isnan(hr_state["bpm"]):
        ax_hr.text(0.02, 0.8, f"HR(avg): {hr_state['bpm']:.1f} bpm",
                   transform=ax_hr.transAxes)
    else:
        ax_hr.text(0.02, 0.8, "HR(avg): ---",
                   transform=ax_hr.transAxes)

    h3, l3 = ax_hr.get_legend_handles_labels()
    if l3:
        ax_hr.legend(loc="upper right")

    fig.canvas.draw()
    fig.canvas.flush_events()

# ===== BLE 유틸 =====

async def write_try(cli: BleakClient, uuid: str, data: bytes, timeout: float = 0.8) -> bool:
    try:
        await asyncio.wait_for(cli.write_gatt_char(uuid, data), timeout=timeout)
        return True
    except Exception as e:
        print(f"[{now_ts()}] write_try 에러: {repr(e)}")
        return False

async def disable_raw(cli: BleakClient):
    try:
        await write_try(cli, RXTX_WRITE_CHARACTERISTIC_UUID, DISABLE_RAW_SENSOR_CMD, 0.5)
    except Exception as e:
        print(f"[{now_ts()}] disable_raw 에러: {repr(e)}")

async def robust_connect(address: str,
                         attempts=8,
                         conn_timeout=20.0,
                         scan_timeout=8.0,
                         adapter='hci0'):
    print(f"[{now_ts()}] BLE robust_connect 시작: addr={address}, adapter={adapter}")
    try:
        from bleak.backends.device import BLEAddressType
        addr_types = [BLEAddressType.PUBLIC, BLEAddressType.RANDOM]
    except Exception:
        addr_types = [None]

    last_err = None
    for n in range(1, attempts+1):
        dev = await BleakScanner.find_device_by_address(
            address,
            timeout=scan_timeout,
            adapter=adapter
        )
        if not dev:
            print(f"[{now_ts()}] 스캔 실패 (시도 {n}/{attempts})")
            await asyncio.sleep(min(1.5*n, 5.0))
            continue

        print(f"[{now_ts()}] 스캔에서 장치 발견: {dev.address} ({dev.name})")

        for at in addr_types:
            try:
                cli = (BleakClient(dev, timeout=conn_timeout, adapter=adapter)
                       if at is None
                       else BleakClient(dev, timeout=conn_timeout,
                                        address_type=at, adapter=adapter))
                try:
                    await asyncio.wait_for(cli.connect(), timeout=conn_timeout+2.0)
                except asyncio.CancelledError:
                    await asyncio.sleep(0.5)
                    raise
                if not cli.is_connected:
                    raise RuntimeError("Connected=False")
                try:
                    await cli.exchange_mtu(247)
                except Exception:
                    pass
                print(f"[{now_ts()}] robust_connect 성공 (addr_type={at})")
                return cli
            except Exception as e:
                last_err = e
                print(f"[{now_ts()}] connect 실패 (addr_type={at}): {repr(e)}")
                await asyncio.sleep(min(0.6*n, 3.0))

    print(f"[{now_ts()}] robust_connect 최종 실패: {repr(last_err)}")
    raise RuntimeError(f"connect failed after {attempts} attempts: {repr(last_err)}")

def _signal_handler(signum, frame):
    global _STOP
    print(f"[{now_ts()}] signal {signum} → stop")
    _STOP = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ===== run: robust_connect + HR/플롯 + TAP(EI) =====
async def run(address: str, csv_path: str, adapter: str = "hci0"):
    global _STOP, hr_hist, last_good_hr, last_good_t
    global last_batt_pct, last_batt_alert_pct, charging_flag, last_status_for_mqtt
    global last_no_wear_t, last_ppg_mono

    init_plot()
    init_mqtt()

    last_status_for_mqtt = None
    loop = asyncio.get_running_loop()

    # === Edge Impulse TAP 모델 초기화 ===
    tap_runner = None
    feature_buf = None
    last_tap_time = None
    feat_count = 75  # 기본값, 모델 로딩 후 업데이트

    try:
        print(f"[{now_ts()}] EI 모델 로딩: {EI_MODEL_PATH}")
        tap_runner = ImpulseRunner(EI_MODEL_PATH)
        model_info = tap_runner.init()
        mp = model_info["model_parameters"]

        axis_count    = mp["axis_count"]
        feat_count    = mp["input_features_count"]
        window_samples = feat_count // axis_count
        labels = mp["labels"]

        print(f"[{now_ts()}] EI model info")
        print(f"  - labels        : {labels}")
        print(f"  - axis_count    : {axis_count}")
        print(f"  - feature_count : {feat_count}")
        print(f"  - window_samples: {window_samples}")

        if "tap_4times" not in labels:
            print(f"[{now_ts()}] WARNING: 'tap_4times' 라벨 없음 → TAP 감지 비활성화")
            tap_runner = None
        else:
            feature_buf = deque(maxlen=feat_count)
    except Exception as e:
        print(f"[{now_ts()}] EI 모델 초기화 실패 → TAP 감지 비활성화: {repr(e)}")
        tap_runner = None
        feature_buf = None

    def do_inference(ts_ms: int):
        nonlocal last_tap_time, feature_buf, tap_runner, feat_count
        global hr_state, charging_flag

        if tap_runner is None or feature_buf is not None and len(feature_buf) < feat_count:
            if tap_runner is None:
                return
        if tap_runner is None or feature_buf is None:
            return
        if len(feature_buf) < feat_count:
            return

        features = list(feature_buf)

        try:
            res = tap_runner.classify(features)
        except Exception as e:
            print(f"[{now_ts()}] EI classify 에러: {repr(e)}")
            return

        result = res.get("result", res)
        classification = result.get("classification", result)

        if not isinstance(classification, dict) or not classification:
            return

        tap_prob = classification.get("tap_4times", 0.0)
        if tap_prob < TAP_THRESHOLD:
            return

        now_mono = time.monotonic()
        # TAP 쿨타임
        if (last_tap_time is not None) and (now_mono - last_tap_time < TAP_COOLTIME_SEC):
            return

        last_tap_time = now_mono
        print(f"[{now_ts()}] TAP DETECTED  prob={tap_prob:.2f}  (ts={ts_ms} ms)")

        # === 상태에 따라 emergency 발행 제한 ===
        # 1) 충전 중이면 TAP 무시
        if charging_flag:
            return

        # 2) NO_WEAR 상태면 TAP 무시
        if hr_state.get("status") == "NO_WEAR":
            return

        # 여기까지 통과했으면 실제 착용 + 비충전 상태 → emergency 발행
        publish_emergency_call()

    print(f"[{now_ts()}] run() 시작")

    while not _STOP:
        try:
            cli = await robust_connect(address,
                                       attempts=8,
                                       conn_timeout=20.0,
                                       scan_timeout=8.0,
                                       adapter=adapter)
        except Exception as e:
            print(f"[{now_ts()}] robust_connect 예외: {repr(e)} → 3초 후 재시도")
            await asyncio.sleep(3.0)
            continue

        t0_mon = time.monotonic()
        ppg_samples.clear()
        buf_ppg_t.clear()
        buf_ppg_v.clear()
        hr_hist.clear()
        hr_state["bpm"] = np.nan
        hr_state["bpm_raw"] = np.nan
        hr_state["status"] = "INIT"
        last_good_hr = np.nan
        last_good_t  = 0.0
        last_batt_pct = None
        last_batt_alert_pct = None
        charging_flag = False
        last_no_wear_t = None
        last_ppg_mono  = None

        # RAW enable 워치독용
        last_enable_raw_mono = None
        raw_rearm_count = 0
        RAW_REARM_LIMIT = 3  # a102/a104 재시도 최대 횟수

        print(f"[{now_ts()}] {address} 연결 완료, RAW 수집 시작")

        f = open(csv_path, "a", newline="", encoding="utf-8")
        w = csv.writer(f)
        w.writerow(["t_wall", "t_rel", "ppg", "bpm", "status"])
        f.flush()

        async def close_all():
            print(f"[{now_ts()}] close_all() 진입")
            try:
                await disable_raw(cli)
            except Exception:
                pass
            try:
                await cli.stop_notify(RXTX_NOTIFY_CHARACTERISTIC_UUID)
            except Exception:
                pass
            try:
                await cli.disconnect()
            except Exception:
                pass
            try:
                f.flush()
                f.close()
            except Exception:
                pass

        # 공통 배터리 처리 함수 (0x03, 0x73 둘 다 이걸 사용)
        def handle_battery_packet(batt_pct: int, charge_bit: int | None):
            nonlocal last_enable_raw_mono, raw_rearm_count
            global last_batt_pct, last_batt_alert_pct, charging_flag

            # 배터리 퍼센트 변화에 따른 No-battery
            if last_batt_pct is None or batt_pct != last_batt_pct:
                if batt_pct < 20:
                    if (last_batt_alert_pct is None) or (batt_pct < last_batt_alert_pct):
                        publish_status("No-battery", hr_state.get("bpm", np.nan))
                        last_batt_alert_pct = batt_pct
                else:
                    last_batt_alert_pct = None

            last_batt_pct = batt_pct

            # === 충전 플래그는 0x73 패킷에서만 갱신 ===
            if charge_bit is not None:
                # 73 0c 5e 01 ... → 충전 시작
                if charge_bit == 0x01:
                    if not charging_flag:
                        charging_flag = True
                        # RAW 끄기 (a102)
                        try:
                            loop.create_task(
                                write_try(
                                    cli,
                                    RXTX_WRITE_CHARACTERISTIC_UUID,
                                    DISABLE_RAW_SENSOR_CMD,
                                    0.5
                                )
                            )
                        except RuntimeError:
                            pass
                        publish_status("charging", hr_state.get("bpm", np.nan))

                # 73 0c 5e 00 ... → 충전 종료
                else:
                    if charging_flag:
                        charging_flag = False
                        from time import monotonic
                        global post_charge_until
                        post_charge_until = monotonic() + 5.0

                        
                        async def _re_enable_raw():
                            nonlocal last_enable_raw_mono, raw_rearm_count
                            # 링이 깨어날 시간 조금 줌
                            await asyncio.sleep(10.0)
                            ok = await write_try(
                                cli,
                                RXTX_WRITE_CHARACTERISTIC_UUID,
                                ENABLE_RAW_SENSOR_CMD,
                                2.0
                            )
                            if ok:
                                print(f"[{now_ts()}] 충전 종료 후 RAW 재활성화 성공(a104)")
                                last_enable_raw_mono = time.monotonic()
                                raw_rearm_count = 0

                        try:
                            loop.create_task(_re_enable_raw())
                        except RuntimeError:
                            pass
                    # 여기서는 status 아무것도 안 보냄
                    # 이후 PPG 들어오면 update_status_and_hr()에서 wear/no-wear 전송

        def on_rxtx(_s, data: bytearray):
            nonlocal t0_mon
            global last_batt_pct, last_batt_alert_pct, charging_flag
            global last_ppg_mono

            if _STOP or not data:
                return

            t_wall = now_ts()
            t_rel  = time.monotonic() - t0_mon
            ts_ms  = int(round(t_rel * 1000.0))

            # 1) PPG & ACC (0xA1 ...)
            if data[0] == 0xA1:
                if len(data) >= 2:
                    subtype = data[1]
                else:
                    return

                # PPG: subtype == 0x02
                if subtype == 0x02 and len(data) >= 4:
                    ppg = (data[2] << 8) | data[3]

                    # 마지막 PPG 수신 시각 갱신 (워치독용)
                    last_ppg_mono = time.monotonic()

                    ppg_samples.append((t_rel, ppg))

                    if (ppg > 0) and (ppg < SAT_VALUE):
                        buf_ppg_t.append(t_rel)
                        buf_ppg_v.append(ppg)

                    while ppg_samples and (t_rel - ppg_samples[0][0] > HR_WINDOW_SEC * 2.0):
                        ppg_samples.popleft()
                    while buf_ppg_t and (t_rel - buf_ppg_t[0] > HR_WINDOW_SEC * 2.0):
                        buf_ppg_t.popleft()
                        buf_ppg_v.popleft()

                    update_status_and_hr(t_rel)

                    bpm_str = "" if np.isnan(hr_state["bpm"]) else f"{hr_state['bpm']:.3f}"
                    w.writerow([t_wall, f"{t_rel:.3f}", ppg, bpm_str, hr_state["status"]])
                    f.flush()
                    return

                # ACC: subtype == 0x03 (EI TAP 감지용)
                elif subtype == 0x03 and len(data) >= 8:
                    ay = s12_to_int((data[2] << 4) | (data[3] & 0x0F))
                    az = s12_to_int((data[4] << 4) | (data[5] & 0x0F))
                    ax = s12_to_int((data[6] << 4) | (data[7] & 0x0F))

                    if feature_buf is not None:
                        feature_buf.extend([float(ax), float(ay), float(az)])
                        do_inference(ts_ms)
                    return

            # 2) Settings / 배터리 & 충전 상태 패킷 (0x73 ...)
            elif data[0] == 0x73 and len(data) >= 4:
                batt_pct   = int(data[2])
                charge_bit = data[3]   # 0x01=충전 중, 0x00=비충전
                handle_battery_packet(batt_pct, charge_bit)
                return

            # 3) 배터리 퍼센트만 알려주는 패킷 (0x03 5e 00 00 ... 61)
            elif data[0] == 0x03 and len(data) >= 2:
                batt_pct = int(data[1])
                handle_battery_packet(batt_pct, None)
                return

        try:
            print(f"[{now_ts()}] Notify 시작 + 설정/RAW 활성화")
            await cli.start_notify(RXTX_NOTIFY_CHARACTERISTIC_UUID, on_rxtx)
            await write_try(cli, RXTX_WRITE_CHARACTERISTIC_UUID, SET_UNITS_METRICS, 0.5)
            # 초기에 RAW ON (충전 아니라고 가정)
            ok_init = await write_try(
                cli,
                RXTX_WRITE_CHARACTERISTIC_UUID,
                ENABLE_RAW_SENSOR_CMD,
                0.8
            )
            if ok_init:
                last_enable_raw_mono = time.monotonic()
                raw_rearm_count = 0

            while not _STOP:
                if not cli.is_connected:
                    print(f"[{now_ts()}] BLE 연결 끊김 → 재연결 루프 진입")
                    break

                now_mono = time.monotonic()

                # ★ a104 후 20초 동안 PPG가 한 번도 안 들어오면 a102 → a104 재전송 (최대 RAW_REARM_LIMIT회)
                if (not charging_flag) and (last_enable_raw_mono is not None) and (raw_rearm_count < RAW_REARM_LIMIT):
                    no_ppg_since_enable = (last_ppg_mono is None) or (last_ppg_mono < last_enable_raw_mono)
                    if no_ppg_since_enable and (now_mono - last_enable_raw_mono > 20.0):
                        print(f"[{now_ts()}] a104 이후 20초간 PPG 없음 → a102/a104 재전송 (회차 {raw_rearm_count+1})")
                        await write_try(
                            cli,
                            RXTX_WRITE_CHARACTERISTIC_UUID,
                            DISABLE_RAW_SENSOR_CMD,
                            0.5
                        )
                        ok_re = await write_try(
                            cli,
                            RXTX_WRITE_CHARACTERISTIC_UUID,
                            ENABLE_RAW_SENSOR_CMD,
                            0.8
                        )
                        raw_rearm_count += 1
                        if ok_re:
                            last_enable_raw_mono = time.monotonic()
                        else:
                            # 실패해도 시간은 갱신해서 너무 자주 때리지 않도록 함
                            last_enable_raw_mono = time.monotonic()

                refresh_plot()
                await asyncio.sleep(0.1)

        except Exception as e:
            print(f"[{now_ts()}] run() 세션 내부 예외: {repr(e)}")

        finally:
            await close_all()

        if _STOP:
            break

        print(f"[{now_ts()}] 3초 후 robust_connect부터 재시도")
        await asyncio.sleep(3.0)

    # 종료시 MQTT/Runner 정리
    global mqtt_client, mqtt_connected
    if mqtt_client is not None:
        try:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        except Exception:
            pass
        mqtt_client = None
        mqtt_connected = False

    # EI runner 종료
    try:
        if tap_runner is not None:
            tap_runner.stop()
    except Exception:
        pass

    print(f"[{now_ts()}] run() 종료")

# ===== main =====
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-a", "--address", default=DEFAULT_ADDR, help="BLE MAC address")
    ap.add_argument("-o", "--out", default=DEFAULT_CSV, help="CSV output path")
    ap.add_argument("--adapter", default="hci0", help="BLE adapter name (default: hci0)")
    args = ap.parse_args()

    try:
        asyncio.run(run(args.address, args.out, args.adapter))
    except KeyboardInterrupt:
        print("KeyboardInterrupt → 종료")
