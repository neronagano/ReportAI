"""Verify a local broker outage without affecting services outside this Compose project."""

import json
import subprocess
import time
from datetime import date
from pathlib import Path
from uuid import uuid4

import httpx

from shiftledger.cli import DEMO_ORGANIZATION_ID
from shiftledger.config import get_settings

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def compose(*arguments: str) -> None:
    subprocess.run(["docker", "compose", *arguments], cwd=PROJECT_ROOT, check=True)


def main() -> None:
    settings = get_settings()
    if not settings.seed_employee_password:
        raise SystemExit("This check requires a seeded local development environment")
    with httpx.Client(base_url="http://127.0.0.1:18000", timeout=15) as client:
        ready = client.get("/health/ready")
        ready.raise_for_status()
        if ready.json().get("service") != "shiftledger":
            raise SystemExit("The target URL is not ShiftLedger")
        login = client.post(
            "/api/v1/auth/login",
            json={
                "email": "employee@example.com",
                "password": settings.seed_employee_password.get_secret_value(),
            },
        )
        login.raise_for_status()
        headers = {"Authorization": "Bearer " + login.json()["access_token"]}
        root = f"/api/v1/organizations/{DEMO_ORGANIZATION_ID}"
        templates = client.get(f"{root}/templates", headers=headers)
        templates.raise_for_status()
        template_id = next(
            template["id"] for template in templates.json() if template["name"] == "Закрытие смены"
        )
        versions = client.get(f"{root}/templates/{template_id}/versions", headers=headers)
        versions.raise_for_status()
        payload = {
            "template_version_id": versions.json()[0]["id"],
            "business_date": date.today().isoformat(),
            "location_code": f"recovery-{uuid4().hex[:12]}",
            "shift_key": "evening",
            "values": {
                "cash": {
                    "opening": "1000.00",
                    "sales": "10.00",
                    "refunds": "0.00",
                    "collected": "0.00",
                    "closing": "1010.00",
                    "revenue": "10.00",
                },
                "expenses": [],
                "notes": {},
            },
        }
        try:
            compose("stop", "kafka")
            submitted = client.post(
                f"{root}/reports",
                headers={**headers, "Idempotency-Key": str(uuid4())},
                json=payload,
            )
            if submitted.status_code != 201:
                raise RuntimeError(
                    f"Report was not accepted during broker outage: {submitted.status_code}"
                )
            report_id = submitted.json()["id"]
            path = f"{root}/reports/{report_id}/reconciliation"
            pending = client.get(path, headers=headers)
            if pending.status_code != 202:
                raise RuntimeError("The report did not remain pending while Kafka was stopped")
            print(
                json.dumps(
                    {"stage": "broker_stopped", "report_accepted": True, "report_id": report_id}
                ),
                flush=True,
            )
        finally:
            compose("start", "kafka")
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            response = client.get(path, headers=headers)
            if response.status_code == 200:
                result = response.json()["result"]
                if result["difference"] != "0.00":
                    raise RuntimeError("Recovered report has an incorrect cash balance")
                print(
                    json.dumps(
                        {
                            "status": "passed",
                            "checks": [
                                "acceptance_during_broker_outage",
                                "eventual_reconciliation_after_recovery",
                            ],
                            "report_id": report_id,
                        }
                    )
                )
                return
            if response.status_code != 202:
                response.raise_for_status()
            time.sleep(0.5)
        raise TimeoutError("Report did not complete within 90 seconds after Kafka recovery")


if __name__ == "__main__":
    main()
