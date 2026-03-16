"""
Travel Receipt Scraper (Groq)
==============================
Pass a PDF → get structured JSON for flight and/or hotel bookings.

Install:
    pip install groq pymupdf python-dotenv

Usage:
    python scraper.py receipt.pdf
    python scraper.py receipt.pdf --type flight
    python scraper.py receipt.pdf --type hotel
"""

import os
import sys
import json
import base64
import argparse
import datetime
from pathlib import Path

import fitz  # PyMuPDF
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))
MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"  # vision capable


# ── PDF → text + images ────────────────────────────────────────────────────────

def extract_pdf(pdf_path: Path) -> tuple[str, list[str]]:
    """Returns (full_text, list_of_base64_png_per_page)."""
    doc = fitz.open(str(pdf_path))
    texts, images = [], []
    for page in doc:
        texts.append(page.get_text("text"))
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), colorspace=fitz.csRGB)
        images.append(base64.standard_b64encode(pix.tobytes("png")).decode())
    doc.close()
    return "\n\n".join(texts), images


# ── Auto-detect receipt type ───────────────────────────────────────────────────

FLIGHT_KW = {"flight","airline","airways","pnr","boarding","departure","arrival","seat","indigo","spicejet","vistara","airindia","air india","akasa"}
HOTEL_KW  = {"hotel","resort","inn","stay","check-in","check-out","checkin","checkout","room","suite","lodge","oyo","marriott","taj","hyatt","hilton"}

def detect_type(text: str) -> str:
    t = text.lower()
    f = any(k in t for k in FLIGHT_KW)
    h = any(k in t for k in HOTEL_KW)
    if f and h: return "both"
    if f: return "flight"
    if h: return "hotel"
    return "unknown"


# ── Prompts ────────────────────────────────────────────────────────────────────

FLIGHT_PROMPT = """Extract flight booking details from this receipt.
Return ONLY a valid JSON object with these exact keys (null if missing):

{
  "traveler_name": "string or null",
  "date_of_booking": "YYYY-MM-DD or null",
  "airline": "string or null",
  "flight_number": "string or null",
  "origin": "city or airport code or null",
  "destination": "city or airport code or null",
  "return_origin": "city or airport code or null (if return flight present)",
  "return_destination": "city or airport code or null (if return flight present)",
  "return_airline": "string or null (if return flight present)",
  "return_flight_number": "string or null (if return flight present)",
  "departure_datetime": "YYYY-MM-DDTHH:MM:SS or null",
  "arrival_datetime": "YYYY-MM-DDTHH:MM:SS or null",
  "seat_class": "Economy/Business/First or null",
  "pnr": "string or null",
  "total_amount": number or null,
  "currency": "INR/USD/etc or null",
  "ota_source": "MakeMyTrip/Yatra/Goibibo/Cleartrip/etc or null"
}

Rules: Only JSON. No markdown. No explanation. total_amount is a plain number."""

HOTEL_PROMPT = """Extract hotel booking details from this receipt.
Return ONLY a valid JSON object with these exact keys (null if missing):

{
  "traveler_name": "string or null",
  "hotel_name": "string or null",
  "date_of_booking": "YYYY-MM-DD or null",
  "checkin_date": "YYYY-MM-DD or null",
  "checkout_date": "YYYY-MM-DD or null",
  "total_amount": number or null,
  "currency": "INR/USD/etc or null",
  "booking_id": "string or null",
  "ota_source": "MakeMyTrip/Yatra/Goibibo/Cleartrip/etc or null"
}

Rules: Only JSON. No markdown. No explanation. total_amount is a plain number."""


# ── LLM call ──────────────────────────────────────────────────────────────────

def call_groq(prompt: str, text: str, images: list[str]) -> dict:
    """Send text + first page image to Groq, return parsed dict."""
    content = []

    # Add first page image for vision grounding
    if images:
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{images[0]}"},
        })

    content.append({
        "type": "text",
        "text": f"{prompt}\n\nReceipt text:\n{text[:8000]}",
    })

    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": content}],
        temperature=0,
        max_tokens=1024,
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown fences if present
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip().rstrip("`").strip()

    return json.loads(raw)


# ── Public functions ───────────────────────────────────────────────────────────

def log_receipt_data(data: dict, receipt_type: str):
    """Appends to scraped_receipts.json"""
    log_file = Path("scraped_receipts.json")
    try:
        if log_file.exists():
            with open(log_file, "r", encoding="utf-8") as f:
                logs = json.load(f)
        else:
            logs = []
    except Exception:
        logs = []
        
    entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "type": receipt_type,
        "data": data
    }
    logs.append(entry)
    
    try:
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Failed to log receipt: {e}")

def extract_flight(pdf_path: str | Path) -> dict:
    """Extract flight booking data from a PDF. Returns a dict."""
    text, images = extract_pdf(Path(pdf_path))
    data = call_groq(FLIGHT_PROMPT, text, images)
    log_receipt_data(data, "flight")
    return data


def extract_hotel(pdf_path: str | Path) -> dict:
    """Extract hotel booking data from a PDF. Returns a dict."""
    text, images = extract_pdf(Path(pdf_path))
    data = call_groq(HOTEL_PROMPT, text, images)
    log_receipt_data(data, "hotel")
    return data


def extract_auto(pdf_path: str | Path) -> dict:
    """
    Auto-detect receipt type and extract accordingly.
    Returns:
        { "flight": {...} }           — flight only
        { "hotel": {...} }            — hotel only
        { "flight": {...}, "hotel": {...} }  — both
    """
    path = Path(pdf_path)
    text, images = extract_pdf(path)
    kind = detect_type(text)

    result = {}

    if kind in ("flight", "both"):
        result["flight"] = call_groq(FLIGHT_PROMPT, text, images)

    if kind in ("hotel", "both"):
        result["hotel"] = call_groq(HOTEL_PROMPT, text, images)

    if kind == "unknown":
        # Try both and return whatever has non-null values
        try:
            result["flight"] = call_groq(FLIGHT_PROMPT, text, images)
        except Exception:
            pass
        try:
            result["hotel"] = call_groq(HOTEL_PROMPT, text, images)
        except Exception:
            pass

    return result


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape travel receipt PDF → JSON")
    parser.add_argument("pdf", type=Path, help="Path to PDF receipt")
    parser.add_argument(
        "--type", "-t",
        choices=["flight", "hotel", "auto"],
        default="auto",
        help="Receipt type (default: auto-detect)",
    )
    args = parser.parse_args()

    if not args.pdf.exists():
        print(f"Error: file not found: {args.pdf}", file=sys.stderr)
        sys.exit(1)

    try:
        if args.type == "flight":
            result = {"flight": extract_flight(args.pdf)}
        elif args.type == "hotel":
            result = {"hotel": extract_hotel(args.pdf)}
        else:
            result = extract_auto(args.pdf)

        print(json.dumps(result, indent=2, ensure_ascii=False))

    except json.JSONDecodeError as e:
        print(f"Error: LLM returned invalid JSON — {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)