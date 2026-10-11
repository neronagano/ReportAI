"""Exercise the running demo stack through HTTP, including private receipt storage."""

import argparse
import json
import time
from datetime import date, timedelta
from io import BytesIO
from uuid import uuid4

import httpx
from PIL import Image

from shiftledger.cli import DEMO_ORGANIZATION_ID
from shiftledger.config import get_settings


def checked(response: httpx.Response, status: int = 200) -> dict | list:
    if response.status_code != status:
        raise RuntimeError(
            f"Expected HTTP {status}, received {response.status_code}: {response.text}"
        )
    return response.json()


def report_values(revenue: str, closing: str, receipt_id: str | None = None) -> dict:
    expense = {
        "amount": "300.00",
        "description": "Purchase of store packaging; synthetic smoke data",
    }
    if receipt_id:
        expense["receipts"] = [receipt_id]
    return {
        "cash": {
            "opening": "1000.00",
            "sales": "5000.00",
            "refunds": "200.00",
            "collected": "2000.00",
            "closing": closing,
            "revenue": revenue,
        },
        "expenses": [expense],
        "notes": {"comment": "Synthetic end-of-shift report"},
    }


def wait_for_result(client: httpx.Client, path: str, headers: dict) -> dict:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        response = client.get(path, headers=headers)
        if response.status_code == 200:
            return checked(response)["result"]
        if response.status_code != 202:
            checked(response)
        time.sleep(0.5)
    raise TimeoutError("Reconciliation was not completed within 60 seconds")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:18000")
    args = parser.parse_args()
    settings = get_settings()
    if not settings.seed_admin_password or not settings.seed_employee_password:
        raise SystemExit("Smoke checks require a locally seeded demo environment")
    with httpx.Client(base_url=args.base_url, timeout=15) as client:
        deadline = time.monotonic() + 60
        while True:
            try:
                ready = client.get("/health/ready")
                if ready.status_code == 200:
                    if ready.json().get("service") != "shiftledger":
                        raise RuntimeError("The target URL belongs to a different application")
                    break
            except (httpx.ConnectError, httpx.TimeoutException):
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError("ShiftLedger API did not become ready within 60 seconds")
            time.sleep(0.5)
        login = checked(
            client.post(
                "/api/v1/auth/login",
                json={
                    "email": "employee@example.com",
                    "password": settings.seed_employee_password.get_secret_value(),
                },
            )
        )
        headers = {"Authorization": f"Bearer {login['access_token']}"}
        root = f"/api/v1/organizations/{DEMO_ORGANIZATION_ID}"
        templates = checked(client.get(f"{root}/templates", headers=headers))
        template = next(item for item in templates if item["name"] == "Закрытие смены")
        versions = checked(
            client.get(f"{root}/templates/{template['id']}/versions", headers=headers)
        )
        version_id = versions[0]["id"]
        # A small synthetic image proves the upload/download path; it is not OCR input.
        image = BytesIO()
        Image.new("RGB", (16, 16), "white").save(image, format="PNG")
        image_bytes = image.getvalue()
        receipt = checked(
            client.post(
                f"{root}/attachments",
                headers=headers,
                files={"file": ("smoke.png", image_bytes, "image/png")},
            ),
            201,
        )
        downloaded = client.get(f"{root}/attachments/{receipt['id']}/content", headers=headers)
        downloaded.raise_for_status()
        if downloaded.content != image_bytes:
            raise RuntimeError("Receipt bytes changed during storage")
        location = f"smoke-{uuid4().hex[:12]}"
        previous_payload = {
            "template_version_id": version_id,
            "business_date": (date.today() - timedelta(days=1)).isoformat(),
            "location_code": location,
            "shift_key": "evening",
            "values": report_values("7000.00", "3500.00"),
        }
        previous = checked(
            client.post(
                f"{root}/reports",
                headers={**headers, "Idempotency-Key": str(uuid4())},
                json=previous_payload,
            ),
            201,
        )
        current_payload = {
            **previous_payload,
            "business_date": date.today().isoformat(),
            "values": report_values("3500.00", "3250.00", receipt["id"]),
        }
        submit_headers = {**headers, "Idempotency-Key": str(uuid4())}
        current = checked(
            client.post(f"{root}/reports", headers=submit_headers, json=current_payload), 201
        )
        repeated = checked(
            client.post(f"{root}/reports", headers=submit_headers, json=current_payload)
        )
        if repeated["id"] != current["id"]:
            raise RuntimeError("Idempotency did not preserve the report ID")
        changed_payload = {
            **current_payload,
            "values": report_values("3600.00", "3250.00", receipt["id"]),
        }
        checked(client.post(f"{root}/reports", headers=submit_headers, json=changed_payload), 409)
        previous_result = wait_for_result(
            client, f"{root}/reports/{previous['id']}/reconciliation", headers
        )
        result = wait_for_result(client, f"{root}/reports/{current['id']}/reconciliation", headers)
        codes = {finding["code"] for finding in result["findings"]}
        if result["difference"] != "-250.00" or not {"cash_shortage", "revenue_drop"}.issubset(
            codes
        ):
            raise RuntimeError(f"Unexpected reconciliation result: {result}")
        if (
            previous_result["difference"] != "0.00"
            or result["baseline_report_id"] != previous["id"]
        ):
            raise RuntimeError("The historical baseline or cash balance is incorrect")
        if result["ai_status"] != "not_configured":
            raise RuntimeError("Unexpected AI configuration status")
        linked = checked(client.get(f"{root}/attachments/{receipt['id']}", headers=headers))
        if linked["report_id"] != current["id"]:
            raise RuntimeError("Receipt was not linked to the submitted report")
        metrics = client.get("/metrics")
        metrics.raise_for_status()
        if "shiftledger_http_requests_total" not in metrics.text:
            raise RuntimeError("HTTP metrics are missing")
        print(
            json.dumps(
                {
                    "status": "passed",
                    "report_id": current["id"],
                    "location_code": location,
                    "difference": result["difference"],
                    "findings": sorted(codes),
                    "checks": [
                        "authentication",
                        "receipt_upload_download",
                        "immutable_submission",
                        "idempotency",
                        "kafka_worker",
                        "cash_reconciliation",
                        "historical_comparison",
                        "metrics",
                    ],
                }
            )
        )


if __name__ == "__main__":
    main()
