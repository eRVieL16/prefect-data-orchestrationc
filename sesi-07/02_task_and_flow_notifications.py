"""
Sesi 7 - Latihan 2: Task-Level Hook & Webhook Dispatcher (Slack / Discord)
--------------------------------------------------------------------------
Konsep Inti:
1. Granularitas Alert:
   - Task Hook: Mendeteksi error pada task spesifik (misal kegagalan API eksternal)
     sebelum flow secara keseluruhan dihentikan.
   - Flow Hook: Mengirim ringkasan status akhir orkestrasi ke tim on-call.
2. Webhook Formatter:
   Menyiapkan format card payload standar (kompatibel dengan Slack / Discord incoming webhook)
   yang mencakup tombol tautan langsung ke UI Prefect untuk investigasi instan.
3. Menghindari Alert Fatigue (Kelelahan Alert):
   Notifikasi sukses HANYA dikirim untuk pipeline batch utama (misal closing bulanan),
   sedangkan pipeline frekuensi tinggi hanya mengirim alert saat GAGAL.
"""

from datetime import datetime, timezone
import json
import os
import urllib.request
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# Otomatis muat environment variables dari file .env di root project
load_dotenv()


def dispatch_webhook_notification(title: str, message: str, color_hex: str, run_id: str, is_failure: bool = True):
    """
    Fungsi pengirim notifikasi webhook universal.
    Mendukung Discord Webhook, Slack Webhook, atau fallback ke simulasi lokal.
    """
    ui_url = f"http://127.0.0.1:4200/runs/flow-run/{run_id}"

    # Format payload Card / Embed (Standar Discord & Slack)
    payload = {
        "content": "🚨 **[PREFECT PRODUCTION ALERT]**" if is_failure else "✅ **[PREFECT PIPELINE SUCCESS]**",
        "embeds": [
            {
                "title": title,
                "description": message,
                "color": int(color_hex.replace("#", ""), 16),
                "fields": [
                    {"name": "Flow Run ID", "value": f"`{run_id}`", "inline": True},
                    {"name": "Dashboard UI", "value": f"[Buka Log Prefect]({ui_url})", "inline": True},
                    {"name": "Waktu", "value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "inline": False}
                ],
                "footer": {"text": "Prefect 3.x Monitoring Engine"}
            }
        ]
    }

    # Cek apakah ada environment variable webhook URL (termasuk dari file .env)
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL") or os.getenv("SLACK_WEBHOOK_URL")

    if webhook_url:
        try:
            req = urllib.request.Request(
                webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                print("\n" + "=" * 70)
                print(f"🚀 [DISCORD WEBHOOK LIVE SENT] HTTP Status: {resp.status}")
                print(f"Judul Alert   : {title}")
                print(f"Tautan UI Log : {ui_url}")
                print("=" * 70 + "\n")
        except Exception as e:
            print(f"[WEBHOOK ERROR] Gagal mengirim ke URL eksternal: {e}")
    else:
        # Simulasi lokal (mencatat ke log dan file)
        log_dir = "./data/alerts"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"webhook_mock_{run_id}.json")
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        print("\n" + "=" * 70)
        print("📨 [SIMULASI WEBHOOK DISPATCHED (Mock Mode)]")
        print(f"Penerima      : Discord / Slack Webhook Channel")
        print(f"Judul         : {title}")
        print(f"Pesan         : {message}")
        print(f"Tautan UI     : {ui_url}")
        print(f"Payload File  : {log_file}")
        print("Tip: Set $env:DISCORD_WEBHOOK_URL untuk mengirim ke channel live!")
        print("=" * 70 + "\n")


# Hook khusus Task Level
def task_failure_alert(task, task_run, state):
    print(f"\n[TASK HOOK TRIGGERED] Task '{task.name}' mengalami kegagalan!")
    dispatch_webhook_notification(
        title=f"Task Failed: {task.name}",
        message=f"Task `{task.name}` gagal dieksekusi: {state.message}",
        color_hex="FF0000",
        run_id=str(task_run.flow_run_id),
        is_failure=True
    )


# Hook khusus Flow Level (Gagal)
def flow_failure_alert(flow, flow_run, state):
    print(f"\n[FLOW HOOK TRIGGERED] Pipeline '{flow.name}' berhenti!")
    dispatch_webhook_notification(
        title=f"Critical Pipeline Failure: {flow.name}",
        message=f"Seluruh orkestrasi dihentikan karena kegagalan unhandled: {state.message}",
        color_hex="8B0000",
        run_id=str(flow_run.id),
        is_failure=True
    )


# =====================================================================
# TASK & FLOW DENGAN KOMBINASI HOOK
# =====================================================================
@task(name="download-remote-sensor-data", on_failure=[task_failure_alert])
def download_sensor_data(sensor_id: str, trigger_error: bool = False):
    logger = get_run_logger()
    logger.info(f"Mengunduh metrik dari IoT Sensor ID: {sensor_id}...")

    if trigger_error:
        raise TimeoutError(f"Sensor '{sensor_id}' tidak merespons (IoT Gateway Timeout).")

    logger.info(f"Sensor {sensor_id} data berhasil diterima: 120 datapoints.")
    return [24.5, 25.1, 24.8]


@flow(
    name="iot-telemetry-pipeline",
    on_failure=[flow_failure_alert],
    description="Pipeline IoT telemetry dengan alert task & flow terpisah."
)
def iot_telemetry_pipeline(sensor_id: str = "SENSOR-JKT-01", fail_sensor: bool = True):
    logger = get_run_logger()
    logger.info(f"Memulai IoT Telemetry Pipeline untuk: {sensor_id}")

    # Task dengan hook tersendiri
    data = download_sensor_data(sensor_id=sensor_id, trigger_error=fail_sensor)
    logger.info(f"Rata-rata suhu sensor: {sum(data)/len(data):.1f}°C")


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("MENGUJI TASK HOOK & WEBHOOK DISPATCHER")
    print("=" * 65)

    try:
        iot_telemetry_pipeline(sensor_id="SENSOR-BDG-99", fail_sensor=True)
    except Exception:
        print("[SELESAI] Task hook dan flow hook keduanya berhasil diuji.")
