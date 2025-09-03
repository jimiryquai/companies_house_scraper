#!/usr/bin/env python3
"""Quick and dirty webhook handler for Snov.io testing.
Receives domain search and email finder results and saves to database.
"""

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict

from flask import Flask, jsonify, request

app = Flask(__name__)

# Database path
DB_PATH = "companies.db"


def log_message(message: str) -> None:
    """Log message with timestamp."""
    print(f"[{datetime.now().isoformat()}] {message}")


def save_domain_result(data: Dict[str, Any]) -> None:
    """Save domain search result to database."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # Extract data from Snov.io response
            request_id = data.get("request_id", "unknown")
            company_name = data.get("company_name", "")
            domain = data.get("domain", "")
            confidence = data.get("confidence", 0)
            success = bool(domain)

            # Insert into company_domains table
            cursor.execute(
                """
                INSERT OR REPLACE INTO company_domains
                (company_number, company_name, domain, confidence, discovered_at, source, request_id)
                VALUES (?, ?, ?, ?, ?, 'snov.io', ?)
            """,
                (
                    request_id,  # Using request_id as company_number for now (we'll map properly later)
                    company_name,
                    domain,
                    confidence,
                    datetime.now().isoformat(),
                    request_id,
                ),
            )

            # Track credit usage
            cursor.execute(
                """
                INSERT INTO snov_credit_usage
                (operation_type, credits_used, company_number, details)
                VALUES ('domain_search', 1, ?, ?)
            """,
                (
                    request_id,  # company_number placeholder
                    f"success={success}, request_id={request_id}",
                ),
            )

            conn.commit()
            log_message(f"Saved domain result: {company_name} -> {domain or 'NOT FOUND'}")

    except Exception as e:
        log_message(f"Error saving domain result: {e}")


def save_snov_domain_results(data: Dict[str, Any]) -> None:
    """Save Snov.io domain search results from webhook."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # Extract from actual Snov.io webhook format
            event_data = data.get("data", {})
            companies = event_data.get("data", [])

            for company in companies:
                company_name = company.get("name", "")
                domains = company.get("result", [])

                for domain_info in domains:
                    domain = (
                        domain_info.get("domain", "")
                        if isinstance(domain_info, dict)
                        else str(domain_info)
                    )
                    confidence = (
                        domain_info.get("confidence", 1.0) if isinstance(domain_info, dict) else 1.0
                    )

                    if domain:
                        cursor.execute(
                            """
                            INSERT OR REPLACE INTO company_domains
                            (company_number, company_name, domain, confidence, discovered_at, source, request_id)
                            VALUES (?, ?, ?, ?, ?, 'snov.io', ?)
                        """,
                            (
                                "unknown",  # We don't have company number from webhook
                                company_name,
                                domain,
                                confidence,
                                datetime.now().isoformat(),
                                data.get("uid", "unknown"),
                            ),
                        )

                        log_message(f"Saved domain: {company_name} -> {domain}")

                # Track API call even if no domains found
                cursor.execute(
                    """
                    INSERT INTO snov_credit_usage
                    (operation_type, credits_used, company_number, details)
                    VALUES ('domain_search', 1, 'unknown', ?)
                """,
                    (
                        f"domains_found={len([c for c in companies if c.get('result')])}, uid={data.get('uid', 'unknown')}"
                    ),
                )

            conn.commit()
            log_message(
                f"Processed {len(companies)} companies, found domains for {sum(1 for c in companies if c.get('result'))}"
            )

    except Exception as e:
        log_message(f"Error saving Snov.io domain results: {e}")


def save_snov_email_results(data: Dict[str, Any]) -> None:
    """Save Snov.io email search results from webhook."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # Extract from actual Snov.io webhook format for emails
            event_data = data.get("data", {})
            results = event_data.get("data", [])

            for result in results:
                email = result.get("email", "")
                first_name = result.get("first_name", "")
                last_name = result.get("last_name", "")
                domain = result.get("domain", "")
                confidence = result.get("confidence", 0.0)

                if email:
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO officer_emails
                        (officer_id, email, confidence, discovered_at, source, request_id)
                        VALUES (?, ?, ?, ?, 'snov.io', ?)
                    """,
                        (
                            "unknown",  # We don't have officer ID from webhook
                            email,
                            confidence,
                            datetime.now().isoformat(),
                            data.get("uid", "unknown"),
                        ),
                    )

                    log_message(f"Saved email: {first_name} {last_name} -> {email}")

            conn.commit()
            log_message(f"Processed {len(results)} email results")

    except Exception as e:
        log_message(f"Error saving Snov.io email results: {e}")


def save_email_result(data: Dict[str, Any]) -> None:
    """Save email finder result to database."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # Extract data from Snov.io response
            request_id = data.get("request_id", "unknown")
            officer_name = data.get("officer_name", "")
            email = data.get("email", "")
            confidence = data.get("confidence", 0)
            success = bool(email)

            # Insert into officer_emails table
            if email:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO officer_emails
                    (officer_id, email, confidence, discovered_at, source, request_id)
                    VALUES (?, ?, ?, ?, 'snov.io', ?)
                """,
                    (
                        request_id,  # Using request_id as officer_id for now
                        email,
                        confidence,
                        datetime.now().isoformat(),
                        request_id,
                    ),
                )

            # Track credit usage
            cursor.execute(
                """
                INSERT INTO snov_credit_usage
                (operation_type, credits_used, company_number, details)
                VALUES ('email_finder', 1, ?, ?)
            """,
                (
                    request_id,  # company_number placeholder
                    f"success={success}, email={email}, officer={officer_name}, request_id={request_id}",
                ),
            )

            conn.commit()
            log_message(f"Saved email result: {officer_name} -> {email or 'NOT FOUND'}")

    except Exception as e:
        log_message(f"Error saving email result: {e}")


@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    """Handle webhook URL validation (GET) and webhooks (POST) from Snov.io."""
    if request.method == "GET":
        # Webhook URL validation - just return 200 OK
        log_message(f"Webhook validation request from {request.remote_addr}")
        return jsonify({"status": "ok", "message": "Webhook URL validated"}), 200

    # Handle POST requests (actual webhooks)
    try:
        data = request.get_json()
        if not data:
            log_message("Received empty webhook payload")
            return jsonify({"status": "error", "message": "Empty payload"}), 400

        log_message(f"Received webhook: {json.dumps(data, indent=2)}")

        # Handle actual Snov.io webhook format
        event_type = data.get("event", "")

        if event_type == "company.found_domains_by_names":
            save_snov_domain_results(data)
        elif event_type == "company.found_emails_by_domain_by_names":
            save_snov_email_results(data)
        elif "domain" in data or "company_name" in data:
            save_domain_result(data)
        elif "email" in data or "officer_name" in data:
            save_email_result(data)
        else:
            log_message(f"Unknown webhook type: {data}")

        return jsonify({"status": "success", "message": "Webhook processed"}), 200

    except Exception as e:
        log_message(f"Error processing webhook: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()}), 200


@app.route("/stats", methods=["GET"])
def stats():
    """Get current test statistics."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # Count domains found
            cursor.execute(
                "SELECT COUNT(*) FROM company_domains WHERE domain IS NOT NULL AND domain != ''"
            )
            domains_found = cursor.fetchone()[0]

            # Count emails found
            cursor.execute(
                "SELECT COUNT(*) FROM officer_emails WHERE email IS NOT NULL AND email != ''"
            )
            emails_found = cursor.fetchone()[0]

            # Count total credits used
            cursor.execute(
                "SELECT SUM(credits_used) FROM snov_credit_usage WHERE timestamp > date('now', '-1 day')"
            )
            credits_used = cursor.fetchone()[0] or 0

            return jsonify(
                {
                    "domains_found": domains_found,
                    "emails_found": emails_found,
                    "credits_used": credits_used,
                    "timestamp": datetime.now().isoformat(),
                }
            ), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    log_message("Starting webhook handler on port 5000...")
    log_message("Use ngrok to expose this publicly: ./ngrok http 5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
