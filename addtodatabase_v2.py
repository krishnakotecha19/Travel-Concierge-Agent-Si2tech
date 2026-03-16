"""
addtodatabase_v2.py
===================
Normalized inserts into si2tech_travel_normalized.

Tables: bookings, employees, booking_passengers, flight_segments,
        hotel_stays, meeting_details

Old module addtodatabase.py and old DB si2tech_travel_master are untouched.

Install:
    pip install psycopg2-binary python-dotenv

.env keys:
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD
    DB_NAME_V2  (defaults to si2tech_travel_normalized)
"""

import os
import re
import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
#  CONNECTION
# ─────────────────────────────────────────────────────────────────────────────

def _get_connection():
    """Returns a psycopg2 connection to the NEW normalized database."""
    return psycopg2.connect(
        host     = os.getenv("DB_HOST",     "localhost"),
        port     = int(os.getenv("DB_PORT", "5432")),
        dbname   = os.getenv("DB_NAME_V2",  "si2tech_travel_normalized"),
        user     = os.getenv("DB_USER",     "postgres"),
        password = os.getenv("DB_PASSWORD", ""),
    )


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date(value) -> datetime.date | None:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        try:
            return datetime.datetime.fromisoformat(value).date()
        except ValueError:
            pass
    return None


def _parse_time(value) -> datetime.time | None:
    if value is None:
        return None
    if isinstance(value, datetime.time):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return datetime.datetime.fromisoformat(value).time()
        except ValueError:
            pass
        for fmt in ("%H:%M:%S", "%H:%M", "%I:%M %p", "%I:%M%p"):
            try:
                return datetime.datetime.strptime(value, fmt).time()
            except ValueError:
                continue
    return None


def _parse_amount(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "").replace("₹", "").replace("Rs.", "").replace("INR", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _split_names(raw: str) -> list[str]:
    """Split comma/ampersand/and-separated employee names into a list."""
    if not raw or not raw.strip():
        return []
    parts = re.split(r'[,&]|\band\b', raw)
    return [p.strip() for p in parts if p.strip()]


# ─────────────────────────────────────────────────────────────────────────────
#  UI INPUTS BUILDER  (same interface as old module)
# ─────────────────────────────────────────────────────────────────────────────

def build_ui_inputs(
    emp_name:         str,
    project_no:       str,
    reason:           str,
    meeting_date,
    meeting_time,
    meeting_location: str,
) -> dict:
    """Packages sidebar UI values into a single dict."""
    return {
        "emp_name":         emp_name         or "",
        "project_no":       project_no       or "",
        "reason":           reason           or "",
        "meeting_date":     _parse_date(meeting_date),
        "meeting_time":     _parse_time(meeting_time),
        "meeting_location": meeting_location or "",
    }


# ─────────────────────────────────────────────────────────────────────────────
#  NORMALIZED INSERT — single transaction across all tables
# ─────────────────────────────────────────────────────────────────────────────

def _get_or_create_employee(cur, name: str) -> int:
    """Returns employee_id for the given name, creating if needed."""
    name = name.strip()
    cur.execute(
        "SELECT employee_id FROM employees WHERE employee_name = %s",
        (name,)
    )
    row = cur.fetchone()
    if row:
        return row["employee_id"]
    cur.execute(
        "INSERT INTO employees (employee_name) VALUES (%s) RETURNING employee_id",
        (name,)
    )
    return cur.fetchone()["employee_id"]


def add_booking(
    scraped_flight: dict | None,
    scraped_return: dict | None,
    scraped_hotel:  dict | None,
    ui:             dict,
) -> dict:
    amounts = []
    if scraped_flight and "error" not in scraped_flight:
        a = _parse_amount(scraped_flight.get("total_amount"))
        if a is not None: amounts.append(a)
    if scraped_return and "error" not in scraped_return:
        a = _parse_amount(scraped_return.get("total_amount"))
        if a is not None: amounts.append(a)
    if scraped_hotel and "error" not in scraped_hotel:
        a = _parse_amount(scraped_hotel.get("total_amount"))
        if a is not None: amounts.append(a)
    total_amount = sum(amounts) if amounts else None

    vendor = "Unknown"
    if scraped_flight and "error" not in scraped_flight:
        vendor = scraped_flight.get("ota_source") or vendor
    elif scraped_hotel and "error" not in scraped_hotel:
        vendor = scraped_hotel.get("ota_source") or vendor

    booking_date = None
    if scraped_flight and "error" not in scraped_flight:
        booking_date = _parse_date(scraped_flight.get("date_of_booking"))
    if not booking_date and scraped_hotel and "error" not in scraped_hotel:
        booking_date = _parse_date(scraped_hotel.get("date_of_booking"))

    emp_names_raw = ui.get("emp_name", "")
    if scraped_flight and "error" not in scraped_flight:
        t = scraped_flight.get("traveler_name", "")
        if t: emp_names_raw = t
    elif scraped_return and "error" not in scraped_return:
        t = scraped_return.get("traveler_name", "")
        if t: emp_names_raw = t
    elif scraped_hotel and "error" not in scraped_hotel:
        t = scraped_hotel.get("traveler_name", "")
        if t: emp_names_raw = t

    emp_names = _split_names(emp_names_raw) or ["Unknown"]

    # ── Shared scope variables — must be defined before both flight blocks ──
    full_route         = []   # outbound route: [origin, ...stops..., destination]
    num_outbound_legs  = 0    # set after outbound block, used by return block

    def _get_connecting_airports(origin, destination, num_legs, airlines, flight_nos, direction="outbound"):
        """Ask Groq for connecting airport(s) between legs."""
        try:
            _groq_key = os.getenv("GROQ_API_KEY")
            if not _groq_key:
                return []
            from groq import Groq as _Groq
            _gc = _Groq(api_key=_groq_key)
            _prompt = (
                f"A connecting {direction} flight goes from {origin} to {destination} "
                f"with {num_legs} legs operated by {', '.join(airlines)} "
                f"with flight numbers {', '.join(flight_nos)}. "
                f"What is the connecting/layover airport IATA code between these legs? "
                f"Reply with ONLY the IATA code(s) comma-separated if multiple. Nothing else."
            )
            _resp = _gc.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": _prompt}],
                max_tokens=20, temperature=0,
            )
            result = [c.strip().upper() for c in _resp.choices[0].message.content.strip().split(",") if c.strip()]
            print(f"  ✅ Groq {direction} connecting airport(s): {result}")
            return result
        except Exception as _e:
            print(f"⚠️ Connecting airport lookup failed ({direction}): {_e}")
            return []

    def _build_route(origin, destination, num_legs, airlines, flight_nos,
                     stop_list=None, fallback_reversed_route=None, direction="outbound"):
        """
        Build full route list [origin, ...stops..., destination].
        Priority:
          1. stop_list from scraper
          2. reversed outbound route (for return legs on same route)
          3. Groq API
          4. Direct origin→destination (no intermediate stops known)
        """
        # 1. Scraper provided stops
        if stop_list:
            route = [origin] + stop_list + [destination]
            route = [route[i] for i in range(len(route)) if i == 0 or route[i] != route[i-1]]
            if len(route) >= num_legs + 1:
                return route

        # Only need connecting airports if more than 1 leg
        if num_legs < 2:
            return [origin, destination]

        # 2. Reverse outbound route (return flight, same cities)
        if fallback_reversed_route and len(fallback_reversed_route) == num_legs + 1:
            print(f"  ↩️ Reusing reversed outbound route for return: {fallback_reversed_route}")
            return fallback_reversed_route

        # 3. Ask Groq
        connecting = _get_connecting_airports(origin, destination, num_legs, airlines, flight_nos, direction)
        if connecting:
            route = [origin] + connecting + [destination]
            if len(route) == num_legs + 1:
                return route

        # 4. Fallback — pad with destination
        route = [origin, destination]
        while len(route) < num_legs + 1:
            route.insert(-1, destination)
        return route

    conn = _get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                # ── 1. Insert booking ──────────────────────────────────────
                cur.execute("""
                    INSERT INTO bookings (project_no, reason, booking_vendor, total_amount, booking_date)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING booking_id, booking_timestamp
                """, (
                    ui.get("project_no", ""),
                    ui.get("reason", ""),
                    vendor,
                    total_amount,
                    booking_date,
                ))
                booking_row = dict(cur.fetchone())
                booking_id  = booking_row["booking_id"]

                # ── 2. Employees ───────────────────────────────────────────
                for name in emp_names:
                    emp_id = _get_or_create_employee(cur, name)
                    cur.execute("""
                        INSERT INTO booking_passengers (booking_id, employee_id)
                        VALUES (%s, %s)
                        ON CONFLICT (booking_id, employee_id) DO NOTHING
                    """, (booking_id, emp_id))

                # ── 3. Outbound flight segments ────────────────────────────
                if scraped_flight and "error" not in scraped_flight:
                    travel_date = (
                        _parse_date(scraped_flight.get("departure_datetime"))
                        or _parse_date(scraped_flight.get("travel_date"))
                        or ui.get("meeting_date")
                    )

                    airlines   = [a.strip() for a in str(scraped_flight.get("airline",       "")).split(",") if a.strip()]
                    flight_nos = [f.strip() for f in str(scraped_flight.get("flight_number", "")).split(",") if f.strip()]
                    pnrs       = [p.strip() for p in str(scraped_flight.get("pnr",           "")).split(",") if p.strip()]

                    raw_stops  = scraped_flight.get("stops", "") or scraped_flight.get("layover_airports", "") or ""
                    stop_list  = [s.strip() for s in str(raw_stops).split(",") if s.strip()]

                    origin      = scraped_flight.get("origin",      "")
                    destination = scraped_flight.get("destination", "")

                    if not airlines:   airlines   = [""]
                    if not flight_nos: flight_nos = [""]
                    if not pnrs:       pnrs       = [""]

                    num_outbound_legs = max(len(airlines), len(flight_nos))

                    full_route = _build_route(
                        origin, destination, num_outbound_legs,
                        airlines, flight_nos,
                        stop_list=stop_list,
                        direction="outbound",
                    )

                    for leg_idx in range(num_outbound_legs):
                        leg_origin = full_route[leg_idx]     if leg_idx     < len(full_route) else origin
                        leg_dest   = full_route[leg_idx + 1] if leg_idx + 1 < len(full_route) else destination

                        cur.execute("""
                            INSERT INTO flight_segments
                                (booking_id, segment_order, airline, flight_number,
                                 origin, destination, travel_date, departure_time, arrival_time,
                                 amount, pnr)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            booking_id,
                            leg_idx + 1,
                            airlines[leg_idx]   if leg_idx < len(airlines)   else airlines[-1],
                            flight_nos[leg_idx] if leg_idx < len(flight_nos) else flight_nos[-1],
                            leg_origin,
                            leg_dest,
                            travel_date,
                            _parse_time(scraped_flight.get("departure_datetime")) if leg_idx == 0                    else None,
                            _parse_time(scraped_flight.get("arrival_datetime"))   if leg_idx == num_outbound_legs - 1 else None,
                            _parse_amount(scraped_flight.get("total_amount"))     if leg_idx == 0                    else None,
                            pnrs[leg_idx] if leg_idx < len(pnrs) else pnrs[-1],
                        ))

                # ── 4. Return flight segments ──────────────────────────────
                if scraped_return and "error" not in scraped_return:
                    ret_travel_date = (
                        _parse_date(scraped_return.get("departure_datetime"))
                        or _parse_date(scraped_return.get("travel_date"))
                    )

                    ret_airlines   = [a.strip() for a in str(scraped_return.get("return_airline")       or scraped_return.get("airline",       "")).split(",") if a.strip()]
                    ret_flight_nos = [f.strip() for f in str(scraped_return.get("return_flight_number") or scraped_return.get("flight_number", "")).split(",") if f.strip()]
                    ret_pnrs       = [p.strip() for p in str(scraped_return.get("pnr") or scraped_return.get("return_pnr", "")).split(",") if p.strip()]

                    ret_origin = scraped_return.get("origin", "")      or (scraped_flight.get("destination", "") if scraped_flight else "")
                    ret_dest   = scraped_return.get("destination", "") or (scraped_flight.get("origin",      "") if scraped_flight else "")

                    raw_ret_stops = scraped_return.get("stops", "") or scraped_return.get("layover_airports", "") or ""
                    ret_stop_list = [s.strip() for s in str(raw_ret_stops).split(",") if s.strip()]

                    if not ret_airlines:   ret_airlines   = [""]
                    if not ret_flight_nos: ret_flight_nos = [""]
                    if not ret_pnrs:       ret_pnrs       = [""]

                    num_ret_legs = max(len(ret_airlines), len(ret_flight_nos))

                    # Pass reversed outbound route as fallback for return
                    reversed_outbound = list(reversed(full_route)) if full_route else None

                    ret_full_route = _build_route(
                        ret_origin, ret_dest, num_ret_legs,
                        ret_airlines, ret_flight_nos,
                        stop_list=ret_stop_list,
                        fallback_reversed_route=reversed_outbound,
                        direction="return",
                    )

                    # segment_order continues from outbound
                    ret_seg_start = num_outbound_legs + 1 if num_outbound_legs > 0 else 1

                    for leg_idx in range(num_ret_legs):
                        leg_origin = ret_full_route[leg_idx]     if leg_idx     < len(ret_full_route) else ret_origin
                        leg_dest   = ret_full_route[leg_idx + 1] if leg_idx + 1 < len(ret_full_route) else ret_dest

                        cur.execute("""
                            INSERT INTO flight_segments
                                (booking_id, segment_order, airline, flight_number,
                                 origin, destination, travel_date, departure_time, arrival_time,
                                 amount, pnr)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            booking_id,
                            ret_seg_start + leg_idx,
                            ret_airlines[leg_idx]   if leg_idx < len(ret_airlines)   else ret_airlines[-1],
                            ret_flight_nos[leg_idx] if leg_idx < len(ret_flight_nos) else ret_flight_nos[-1],
                            leg_origin,
                            leg_dest,
                            ret_travel_date,
                            _parse_time(scraped_return.get("departure_datetime")) if leg_idx == 0               else None,
                            _parse_time(scraped_return.get("arrival_datetime"))   if leg_idx == num_ret_legs - 1 else None,
                            _parse_amount(scraped_return.get("total_amount"))     if leg_idx == 0               else None,
                            ret_pnrs[leg_idx] if leg_idx < len(ret_pnrs) else ret_pnrs[-1],
                        ))

                # ── 5. Hotel stay ──────────────────────────────────────────
                if scraped_hotel and "error" not in scraped_hotel:
                    cur.execute("""
                        INSERT INTO hotel_stays
                            (booking_id, hotel_name, check_in_date, checkout_date,
                             amount, booking_id_ext)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        booking_id,
                        scraped_hotel.get("hotel_name", ""),
                        _parse_date(scraped_hotel.get("checkin_date")),
                        _parse_date(scraped_hotel.get("checkout_date")),
                        _parse_amount(scraped_hotel.get("total_amount")),
                        scraped_hotel.get("booking_id", ""),
                    ))

                # ── 6. Meeting details ─────────────────────────────────────
                cur.execute("""
                    INSERT INTO meeting_details
                        (booking_id, meeting_date, meeting_time, meeting_location)
                    VALUES (%s, %s, %s, %s)
                """, (
                    booking_id,
                    ui.get("meeting_date"),
                    ui.get("meeting_time"),
                    ui.get("meeting_location", ""),
                ))

        print(f"✅ Booking saved (normalized) → booking_id={booking_id}")
        return booking_row

    except Exception as e:
        print(f"❌ Normalized DB error: {e}")
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  READ FUNCTIONS — fetch transaction history from normalized DB
# ─────────────────────────────────────────────────────────────────────────────

def fetch_flight_transactions() -> list[dict]:
    """
    Returns flight segments with booking info + individual passenger rows.
    One row per (segment × passenger) — caller pivots passengers into columns.
    """
    conn = _get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    b.booking_id,
                    b.project_no,
                    b.booking_date,
                    fs.airline,
                    fs.flight_number,
                    fs.origin,
                    fs.destination,
                    fs.travel_date,
                    fs.amount          AS segment_amount,
                    fs.pnr,
                    e.employee_name    AS passenger
                FROM flight_segments fs
                JOIN bookings              b  ON b.booking_id  = fs.booking_id
                LEFT JOIN booking_passengers bp ON bp.booking_id = b.booking_id
                LEFT JOIN employees          e  ON e.employee_id = bp.employee_id
                ORDER BY b.booking_timestamp DESC, fs.segment_order, e.employee_name
            """)
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"❌ fetch_flight_transactions error: {e}")
        raise
    finally:
        conn.close()


def fetch_hotel_transactions() -> list[dict]:
    """
    Returns hotel stays with booking info + individual passenger rows.
    One row per (stay × passenger) — caller pivots passengers into columns.
    """
    conn = _get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    b.booking_id,
                    b.project_no,
                    b.reason,
                    b.booking_date,
                    hs.hotel_name,
                    hs.amount          AS hotel_amount,
                    e.employee_name    AS passenger
                FROM hotel_stays hs
                JOIN bookings              b  ON b.booking_id  = hs.booking_id
                LEFT JOIN booking_passengers bp ON bp.booking_id = b.booking_id
                LEFT JOIN employees          e  ON e.employee_id = bp.employee_id
                ORDER BY b.booking_timestamp DESC, e.employee_name
            """)
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"❌ fetch_hotel_transactions error: {e}")
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  REVERT — delete a booking and all its related rows (CASCADE handles FKs)
# ─────────────────────────────────────────────────────────────────────────────

def revert_booking(booking_id: int) -> bool:
    """
    Deletes a booking by ID. CASCADE on foreign keys automatically removes
    related rows from booking_passengers, flight_segments, hotel_stays,
    and meeting_details.
    Returns True if a row was deleted, False if booking_id not found.
    """
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM bookings WHERE booking_id = %s", (booking_id,))
                deleted = cur.rowcount > 0
        if deleted:
            print(f"🗑️ Reverted booking_id={booking_id} (and all related rows)")
        else:
            print(f"⚠️ booking_id={booking_id} not found — nothing to revert")
        return deleted
    except Exception as e:
        print(f"❌ Revert error: {e}")
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  BACKWARD-COMPATIBLE WRAPPERS (so finaltestcode.py can use same interface)
# ─────────────────────────────────────────────────────────────────────────────

def add_flight_booking(scraped: dict, ui: dict) -> dict:
    """Wrapper: inserts outbound flight only (compatible with old interface)."""
    return add_booking(
        scraped_flight=scraped,
        scraped_return=None,
        scraped_hotel=None,
        ui=ui,
    )


def add_hotel_booking(scraped: dict, ui: dict) -> dict:
    """Wrapper: inserts hotel only (compatible with old interface)."""
    return add_booking(
        scraped_flight=None,
        scraped_return=None,
        scraped_hotel=scraped,
        ui=ui,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  DATA MIGRATION — from old DB to new normalized DB
# ─────────────────────────────────────────────────────────────────────────────

def migrate_from_old_db():
    """
    Reads from si2tech_travel_master (old) and inserts into
    si2tech_travel_normalized (new).

    Run: python addtodatabase_v2.py migrate
    """
    import psycopg2.extras

    old_conn = psycopg2.connect(
        host     = os.getenv("DB_HOST",     "localhost"),
        port     = int(os.getenv("DB_PORT", "5432")),
        dbname   = os.getenv("DB_NAME",     "si2tech_travel_master"),
        user     = os.getenv("DB_USER",     "postgres"),
        password = os.getenv("DB_PASSWORD", ""),
    )

    new_conn = _get_connection()

    try:
        with old_conn, old_conn.cursor(cursor_factory=RealDictCursor) as old_cur:
            # ── Migrate flight_bookings ────────────────────────────────────
            old_cur.execute("SELECT * FROM flight_bookings ORDER BY id")
            flight_rows = old_cur.fetchall()
            print(f"📦 Migrating {len(flight_rows)} flight booking(s)...")

            with new_conn:
                with new_conn.cursor(cursor_factory=RealDictCursor) as cur:
                    for row in flight_rows:
                        # 1. Create booking
                        cur.execute("""
                            INSERT INTO bookings
                                (booking_timestamp, project_no, reason, booking_vendor,
                                 total_amount, booking_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            RETURNING booking_id
                        """, (
                            row.get("booking_timestamp"),
                            row.get("project_no", ""),
                            row.get("reason", ""),
                            row.get("booking_vendor", "Unknown"),
                            row.get("amount"),
                            row.get("booking_date"),
                        ))
                        bid = cur.fetchone()["booking_id"]

                        # 2. Employees
                        names = _split_names(row.get("emp_name", ""))
                        if not names:
                            names = ["Unknown"]
                        for name in names:
                            eid = _get_or_create_employee(cur, name)
                            cur.execute("""
                                INSERT INTO booking_passengers (booking_id, employee_id)
                                VALUES (%s, %s)
                                ON CONFLICT (booking_id, employee_id) DO NOTHING
                            """, (bid, eid))

                        # 3. Outbound segment
                        cur.execute("""
                            INSERT INTO flight_segments
                                (booking_id, segment_order, airline, flight_number,
                                 origin, destination, travel_date, departure_time, arrival_time)
                            VALUES (%s, 1, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            bid,
                            row.get("airline", ""),
                            row.get("flight_no", ""),
                            row.get("origin", ""),
                            row.get("destination", ""),
                            row.get("travel_date"),
                            row.get("departure"),
                            row.get("arrival"),
                        ))

                        # 4. Return segment (if present)
                        ret_airline = row.get("return_airline", "")
                        ret_fno     = row.get("return_flight_number", "")
                        ret_origin  = row.get("return_origin", "")
                        ret_dest    = row.get("return_destination", "")
                        if ret_airline or ret_fno or ret_origin or ret_dest:
                            cur.execute("""
                                INSERT INTO flight_segments
                                    (booking_id, segment_order, airline, flight_number,
                                     origin, destination, travel_date)
                                VALUES (%s, 2, %s, %s, %s, %s, %s)
                            """, (
                                bid, ret_airline, ret_fno, ret_origin, ret_dest,
                                row.get("return_date"),
                            ))

                        # 5. Meeting details
                        cur.execute("""
                            INSERT INTO meeting_details
                                (booking_id, meeting_date, meeting_time, meeting_location)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            bid,
                            row.get("meeting_date"),
                            row.get("meeting_time"),
                            row.get("meeting_location", ""),
                        ))

                    # ── Migrate hotel_bookings ─────────────────────────────
                    old_cur.execute("SELECT * FROM hotel_bookings ORDER BY id")
                    hotel_rows = old_cur.fetchall()
                    print(f"📦 Migrating {len(hotel_rows)} hotel booking(s)...")

                    for row in hotel_rows:
                        cur.execute("""
                            INSERT INTO bookings
                                (booking_timestamp, project_no, reason, booking_vendor,
                                 total_amount, booking_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            RETURNING booking_id
                        """, (
                            row.get("booking_timestamp"),
                            row.get("project_no", ""),
                            row.get("reason", ""),
                            row.get("booking_vendor", "Unknown"),
                            row.get("amount"),
                            row.get("booking_date"),
                        ))
                        bid = cur.fetchone()["booking_id"]

                        names = _split_names(row.get("emp_name", ""))
                        if not names:
                            names = ["Unknown"]
                        for name in names:
                            eid = _get_or_create_employee(cur, name)
                            cur.execute("""
                                INSERT INTO booking_passengers (booking_id, employee_id)
                                VALUES (%s, %s)
                                ON CONFLICT (booking_id, employee_id) DO NOTHING
                            """, (bid, eid))

                        cur.execute("""
                            INSERT INTO hotel_stays
                                (booking_id, hotel_name, check_in_date, checkout_date, amount)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            bid,
                            row.get("hotel_name", ""),
                            row.get("check_in_date"),
                            row.get("checkout_date"),
                            row.get("amount"),
                        ))

                        cur.execute("""
                            INSERT INTO meeting_details
                                (booking_id, meeting_date, meeting_time, meeting_location)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            bid,
                            row.get("meeting_date"),
                            row.get("meeting_time"),
                            row.get("meeting_location", ""),
                        ))

        print("✅ Migration complete!")
    except Exception as e:
        print(f"❌ Migration error: {e}")
        raise
    finally:
        old_conn.close()
        new_conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) > 1 and sys.argv[1] == "migrate":
        print("=== Starting migration from old DB → normalized DB ===")
        migrate_from_old_db()
        sys.exit(0)

    # Default: run test with mock data
    print("=== Testing normalized insert ===")

    mock_flight = {
        "traveler_name":      "Rahul Sharma, Priya Patel",
        "date_of_booking":    "2026-03-10",
        "airline":            "IndiGo",
        "flight_number":      "6E-234",
        "origin":             "BOM",
        "destination":        "DEL",
        "departure_datetime": "2026-03-15T07:30:00",
        "arrival_datetime":   "2026-03-15T09:45:00",
        "pnr":                "XY1234",
        "total_amount":       4500,
        "currency":           "INR",
        "ota_source":         "MakeMyTrip",
    }

    mock_return = {
        "airline":            "IndiGo",
        "flight_number":      "6E-567",
        "origin":             "DEL",
        "destination":        "BOM",
        "departure_datetime": "2026-03-16T18:00:00",
        "arrival_datetime":   "2026-03-16T20:15:00",
        "pnr":                "XY5678",
        "total_amount":       4200,
        "traveler_name":      "Rahul Sharma, Priya Patel",
    }

    mock_hotel = {
        "traveler_name":   "Rahul Sharma",
        "hotel_name":      "Taj Lands End",
        "date_of_booking": "2026-03-10",
        "checkin_date":    "2026-03-15",
        "checkout_date":   "2026-03-16",
        "total_amount":    8500,
        "currency":        "INR",
        "booking_id":      "HTL-9876",
        "ota_source":      "Booking.com",
    }

    mock_ui = build_ui_inputs(
        emp_name         = "Rahul Sharma, Priya Patel",
        project_no       = "PRJ-001",
        reason           = "Client Meeting",
        meeting_date     = datetime.date(2026, 3, 15),
        meeting_time     = datetime.time(14, 0),
        meeting_location = "Connaught Place, Delhi",
    )

    try:
        result = add_booking(mock_flight, mock_return, mock_hotel, mock_ui)
        print(json.dumps(result, default=str, indent=2))
        print("\n✅ Test passed — 2 employees, 2 flight segments, 1 hotel, 1 meeting inserted!")
    except Exception as e:
        print(f"Test failed: {e}")
