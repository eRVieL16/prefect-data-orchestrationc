"""
Sesi 7 - Latihan 1: Alerting dengan State Change Hooks (on_failure)
-------------------------------------------------------------------
Konsep Inti:
1. Zero-Noise Alerting:
   Pipeline yang sukses tidak perlu membombardir channel komunikasi dengan spam.
   Notifikasi hanya dikirim saat kondisi abnormal / fatal ('Failed' atau 'Crashed').
2. Prefect Flow Hook:
   Fungsi callback '@flow(on_failure=[...])' yang otomatis dipanggil oleh Prefect engine
   segera setelah flow run memasuki state Failed.
3. Struktur Pesan Insiden yang Actionable:
   Pesan alert yang baik HARUS memuat:
   - Nama flow & Flow Run ID
   - State & Ringkasan Error
   - Link langsung ke Prefect UI untuk investigasi lebih lanjut
   - Timestamp kejadian
"""

from datetime import datetime, timezone
import json
import os
import urllib.request
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# Muat environment variables dari .env
load_dotenv()


def send_incident_alert(flow, flow_run, state):
    """
    Hook callback yang dieksekusi saat flow gagal.
    Mengekstrak informasi kegagalan dan membuat laporan insiden.
    """
    error_message = state.message or "Unknown exception occurred."
    ui_url = f"http://127.0.0.1:4200/runs/flow-run/{flow_run.id}"
    timestamp = datetime.now(timezone.utc).isoformat()

    # Struktur payload insiden standar (format JSON untuk Slack / Discord / PagerDuty)
    alert_payload = {
        "event": "PIPELINE_FAILURE",
        "severity": "CRITICAL",
        "timestamp": timestamp,
        "flow_name": flow.name,
        "flow_run_name": flow_run.name,
        "flow_run_id": str(flow_run.id),
        "state": state.name,
        "error_summary": str(error_message),
        "dashboard_url": ui_url
    }

    # Simpan laporan insiden ke file lokal disk
    alerts_dir = "./data/alerts"
    os.makedirs(alerts_dir, exist_ok=True)
    report_path = os.path.join(alerts_dir, f"incident_{flow_run.id}.json")

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(alert_payload, f, indent=2)

    # Cetak banner alert insiden di console
    print("\n" + "!" * 75)
    print("🚨 [CRITICAL ALERT DISPATCHED - ON_FAILURE HOOK] 🚨")
    print(f"Flow Name     : {flow.name}")
    print(f"Flow Run ID   : {flow_run.id}")
    print(f"State         : {state.name}")
    print(f"Ringkasan     : {error_message}")
    print(f"Investigasi UI: {ui_url}")
    print(f"Laporan File  : {report_path}")
    print("!" * 75 + "\n")

    # Jika DISCORD_WEBHOOK_URL disetel, kirim pesan langsung ke Discord channel
    discord_url = os.getenv("DISCORD_WEBHOOK_URL")
    if discord_url:
        discord_body = {
            "content": f"🚨 **[INCIDENT ALERT: {flow.name}]**",
            "embeds": [
                {
                    "title": f"Flow Failed: {flow.name}",
                    "description": str(error_message),
                    "color": 15158332,  # Merah (Red)
                    "fields": [
                        {"name": "Flow Run Name", "value": f"`{flow_run.name}`", "inline": True},
                        {"name": "Flow Run ID", "value": f"`{flow_run.id}`", "inline": True},
                        {"name": "Dashboard Log", "value": f"[Buka di Prefect UI]({ui_url})", "inline": False}
                    ],
                    "footer": {"text": "Prefect Sesi 7 - Production Incident Response"}
                }
            ]
        }
        try:
            req = urllib.request.Request(
                discord_url,
                data=json.dumps(discord_body).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(f"🚀 [DISCORD LIVE SENT] Status HTTP: {resp.status} - Alert berhasil mendarat di channel!")
        except Exception as e:
            print(f"[DISCORD SEND ERROR] Gagal mengirim: {e}")


# =====================================================================
# TASK-TASK PIPELINE PENYELESAIAN PEMBAYARAN (SETTLEMENT)
# =====================================================================
@task(name="verify-bank-gateway")
def verify_bank_gateway(simulate_failure: bool = False) -> bool:
    logger = get_run_logger()
    logger.info("[STEP 1] Menghubungi payment gateway API bank mitra...")

    if simulate_failure:
        logger.error("[GATEWAY ERROR] Koneksi gateway bank menolak koneksi (HTTP 503 Service Unavailable)!")
        raise ConnectionError("Bank Partner API timeout setelah 30 detik (HTTP 503).")

    logger.info("[STEP 1] Gateway bank online dan siap memproses settlement.")
    return True


@task(name="process-merchant-settlement")
def process_merchant_settlement(merchant_id: str, amount: float) -> str:
    logger = get_run_logger()
    logger.info(f"[STEP 2] Memproses transfer dana Rp {amount:,.0f} ke merchant {merchant_id}...")
    settlement_ref = f"SETTLE-{merchant_id}-998"
    logger.info(f"[STEP 2] Transfer sukses dengan No Ref: {settlement_ref}")
    return settlement_ref


# =====================================================================
# MAIN FLOW DENGAN HOOK ON_FAILURE
# =====================================================================
@flow(
    name="payment-settlement-flow",
    on_failure=[send_incident_alert],
    description="Pipeline settlement pembayaran dengan otomatisasi alert kegagalan."
)
def payment_settlement_flow(
    merchant_id: str = "MERCHANT-88",
    amount: float = 75000000.0,
    simulate_failure: bool = True
):
    """
    Main flow yang dilengkapi hook on_failure.
    Jika simulate_failure=True, flow akan gagal dan otomatis memicu send_incident_alert.
    """
    logger = get_run_logger()
    logger.info("==================================================")
    logger.info(f"Memulai Settlement Pipeline untuk {merchant_id}")
    logger.info(f"Nominal: Rp {amount:,.0f} | Simulasi Gagal: {simulate_failure}")
    logger.info("==================================================")

    # Langkah 1: Verifikasi gateway (akan melempar ConnectionError jika simulate_failure=True)
    verify_bank_gateway(simulate_failure=simulate_failure)

    # Langkah 2: Proses transfer jika gateway aman
    ref = process_merchant_settlement(merchant_id=merchant_id, amount=amount)
    logger.info(f"Settlement Selesai! No Referensi: {ref}")
    return ref


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("MENGUJI COBA FLOW HOOK ON_FAILURE (SKENARIO KEGAGALAN)")
    print("=" * 65)

    try:
        # Jalankan dengan simulasi kegagalan aktif
        payment_settlement_flow(simulate_failure=True)
    except Exception as e:
        print(f"\n[INFO] Exception tertangkap di blok utama: {type(e).__name__}")
        print("Flow memasuki state Failed dan memicu hook alert secara otomatis!")
