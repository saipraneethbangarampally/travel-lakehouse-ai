"""Synthetic source systems that mimic the real problem: three white-label partners, each
with its own on-prem database and schema, plus marketing-tool exports, Tealium web events and a consent stream.

Day 1 = initial load. Day 2 = an incremental day with tier changes (SCD2), moves, new
members, cancellations, late-arriving members, source data fixes and consent opt-outs.

All people are fictional; emails use reserved example.* domains.
"""
from __future__ import annotations

import argparse
import calendar
import json
import random
import sqlite3
from datetime import date, datetime, timedelta

from . import settings

SEED = 42
REF = {1: datetime(2026, 11, 1, 2, 0, 0), 2: datetime(2026, 11, 2, 2, 0, 0)}

FIRST = ["Ava", "Liam", "Mia", "Noah", "Emma", "Lucas", "Zoe", "Ethan", "Lena", "Omar",
         "Priya", "Mateo", "Hana", "Felix", "Chloe", "Ravi", "Sofia", "Jonas", "Amara",
         "Diego", "Ines", "Kai", "Nora", "Elias", "Maya", "Theo", "Leila", "Arjun"]
LAST = ["Garcia", "Smith", "Nguyen", "Muller", "Martin", "Patel", "Kim", "Rossi", "Silva",
        "Brown", "Dubois", "Schmidt", "Lopez", "Walker", "Khan", "Jensen", "Moreau",
        "Okafor", "Reyes", "Fischer", "Hughes", "Tanaka", "Costa", "Bauer"]
STATES = {"US": ["AZ", "TX", "CA", "FL", "NY", "WA", "IL"], "CA": ["ON", "BC", "QC"],
          "MX": ["CMX", "JAL"], "GB": ["ENG", "SCT"], "DE": ["BE", "BY"], "FR": ["IDF", "PAC"]}
DESTS = {"CRUISE": ["CARIB", "ALASKA", "MEDIT", "MEXRIV"], "HOTEL": ["LAS", "CUN", "PAR", "NYC", "LIS"],
         "CAR": ["PHX", "MCO", "LAX"], "FLIGHT": ["LHR", "JFK", "CDG", "SEA"]}
PRICE = {"CRUISE": (900, 6000), "HOTEL": (150, 2200), "CAR": (80, 900), "FLIGHT": (120, 1800)}
CAMPAIGNS = {
    "BHB": ["BHB-CRZ-ALASKA-26", "BHB-CAR-SUMMER-26", "BHB-HTL-VEGAS-26"],
    "SAM": ["SAM-CRZ-CARIB-26", "SAM-HTL-EUCITY-26", "SAM-FLT-SALE-26"],
    "NWCU": ["NWCU-CRZ-MEXRIV-26", "NWCU-HTL-BEACH-26"],
}
PARTNER_SHAPE = {  # members, country weights, tier codes
    "BHB": (1200, {"US": 0.92, "CA": 0.08}, ["B", "S", "G", "P"]),
    "SAM": (1500, {"US": 0.45, "GB": 0.18, "DE": 0.14, "FR": 0.11, "CA": 0.07, "MX": 0.05},
            ["blue", "silver", "gold", "diamond"]),
    "NWCU": (900, {"US": 0.85, "CA": 0.15}, ["STD", "PRF", "PRV"]),
}
ISO3 = {"US": "USA", "CA": "CAN", "MX": "MEX", "GB": "GBR", "DE": "DEU", "FR": "FRA"}
LATE_ARRIVING = 4  # members per partner whose row lands a day after their bookings


# ---------------------------------------------------------------- canonical generator --

def _people(partner: str, rng: random.Random):
    n, weights, tiers = PARTNER_SHAPE[partner]
    countries, w = zip(*weights.items())
    people = []
    for i in range(1, n + 1 + LATE_ARRIVING):
        fn, ln = rng.choice(FIRST), rng.choice(LAST)
        c = rng.choices(countries, w)[0]
        created = REF[1] - timedelta(days=rng.randint(30, 2500), seconds=rng.randint(0, 86399))
        people.append({
            "id": i, "first": fn, "last": ln,
            "email": f"{fn.lower()}.{ln.lower()}{i}@example.{rng.choice(['com', 'org', 'net'])}",
            "phone": f"+1-555-01{rng.randint(10, 99)}-{rng.randint(1000, 9999)}",
            "country": c, "state": rng.choice(STATES[c]),
            "tier": rng.choices(tiers, [0.55, 0.25, 0.15, 0.05][: len(tiers)])[0],
            "created": created,
            "modified": created + timedelta(days=rng.randint(0, 20)),
            "late": i > n,  # late-arriving: inserted on day 2
        })
    return people


def _bookings(partner: str, people, rng: random.Random, start_id: int, n_per_person=(0, 5),
              window=(REF[1] - timedelta(days=900), REF[1] - timedelta(hours=3))):
    out, bid = [], start_id
    span = int((window[1] - window[0]).total_seconds())
    for p in people:
        k = rng.randint(*n_per_person)
        if p["late"]:
            k = max(k, 1)
        for _ in range(k):
            product = rng.choices(list(DESTS), [0.22, 0.4, 0.2, 0.18])[0]
            booked = window[0] + timedelta(seconds=rng.randint(0, span))
            travel = booked.date() + timedelta(days=rng.randint(7, 300))
            status = "COMPLETED" if travel < REF[1].date() else "CONFIRMED"
            if rng.random() < 0.06:
                status = "CANCELLED"
            out.append({"id": bid, "member": p["id"], "product": product,
                        "dest": rng.choice(DESTS[product]), "booked": booked, "travel": travel,
                        "amount": round(rng.uniform(*PRICE[product]), 2),
                        "currency": "USD", "status": status,
                        "modified": booked + timedelta(minutes=rng.randint(1, 90))})
            bid += 1
    return out


# ---------------------------------------------------------------- partner writers ------

def _fmt(partner: str, dt: datetime) -> str:
    if partner == "BHB":
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    if partner == "SAM":
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%Y%m%d%H%M%S")


def _member_row(partner: str, p: dict):
    m = _fmt(partner, p["modified"])
    if partner == "BHB":
        return (p["id"], p["first"], p["last"], p["email"], p["phone"], p["country"],
                p["state"], p["tier"], p["created"].strftime("%Y-%m-%d %H:%M:%S"), m)
    if partner == "SAM":
        return (f"S{p['id']:06d}", f"{p['first']} {p['last']}", p["email"], p["phone"],
                p["country"].lower(), p["state"], p["tier"],
                p["created"].strftime("%Y-%m-%dT%H:%M:%SZ"), m)
    return (f"N{p['id']:05d}", p["first"].upper(), p["last"].upper(), p["email"],
            p.get("iso3") or ISO3[p["country"]], p["state"], p["tier"],
            p["created"].strftime("%Y%m%d"), m)


def _booking_row(partner: str, b: dict):
    m = _fmt(partner, b["modified"])
    if partner == "BHB":
        return (b["id"], b["member"], b["product"].title(), b["dest"],
                b["booked"].strftime("%Y-%m-%d"), b["travel"].isoformat(), b["amount"],
                b["status"].title(), m)
    if partner == "SAM":
        cat = {"CRUISE": "cruise", "HOTEL": "hotel", "CAR": "car", "FLIGHT": "air"}[b["product"]]
        st = {"CONFIRMED": "ok", "CANCELLED": "cxl", "COMPLETED": "done"}[b["status"]]
        cur = b["currency"]
        amt = b["amount"] / {"USD": 1, "EUR": 1.08, "GBP": 1.27, "CAD": 0.73, "MXN": 0.055,
                             "JPY": 0.0066}[cur]
        return (f"R{b['id']:07d}", f"S{b['member']:06d}", cat, b["dest"],
                b["booked"].strftime("%Y-%m-%d"), b["travel"].isoformat(),
                int(round(amt * 100)), cur, st, m)
    code = {"CRUISE": "C", "HOTEL": "H", "CAR": "R", "FLIGHT": "A"}[b["product"]]
    st = {"CONFIRMED": "A", "CANCELLED": "X", "COMPLETED": "C"}[b["status"]]
    return (f"T{b['id']:07d}", f"N{b['member']:05d}", code, b["dest"],
            b["booked"].strftime("%Y%m%d"), b["travel"].strftime("%Y%m%d"), b["amount"], st, m)


DDL = {
    "BHB": [
        "CREATE TABLE Members (MemberID INTEGER PRIMARY KEY, FirstName TEXT, LastName TEXT, "
        "EmailAddress TEXT, Phone TEXT, Country TEXT, State TEXT, TierCode TEXT, CreatedDate TEXT, ModifiedDate TEXT)",
        "CREATE TABLE Bookings (BookingID INTEGER PRIMARY KEY, MemberID INTEGER, ProductType TEXT, "
        "DestinationCode TEXT, BookingDate TEXT, TravelDate TEXT, Amount_USD REAL, Status TEXT, ModifiedDate TEXT)",
    ],
    "SAM": [
        "CREATE TABLE customer (cust_id TEXT PRIMARY KEY, full_name TEXT, email TEXT, phone_number TEXT, "
        "country_iso TEXT, region TEXT, loyalty_level TEXT, created_at TEXT, updated_at TEXT)",
        "CREATE TABLE reservation (res_id TEXT PRIMARY KEY, cust_id TEXT, category TEXT, dest TEXT, "
        "booked_on TEXT, depart_on TEXT, total_cents INTEGER, currency TEXT, state TEXT, updated_at TEXT)",
    ],
    "NWCU": [
        "CREATE TABLE mbr (mbr_no TEXT PRIMARY KEY, nm_first TEXT, nm_last TEXT, email_addr TEXT, "
        "cntry TEXT, st TEXT, segment TEXT, open_dt TEXT, last_upd_ts TEXT)",
        "CREATE TABLE trips (trip_no TEXT PRIMARY KEY, mbr_no TEXT, trip_type TEXT, dest_cd TEXT, "
        "book_dt TEXT, travel_dt TEXT, amt REAL, status_cd TEXT, last_upd_ts TEXT)",
    ],
}
TABLES = {"BHB": ("Members", "Bookings"), "SAM": ("customer", "reservation"), "NWCU": ("mbr", "trips")}


def _upsert_rows(conn, table: str, rows):
    if not rows:
        return
    ph = ",".join("?" * len(rows[0]))
    conn.executemany(f"INSERT OR REPLACE INTO {table} VALUES ({ph})", rows)


# ---------------------------------------------------------------- marketing + consent --

def _jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _engagement(partner, people, rng, start, end, per_person, start_id):
    events, aid = [], start_id
    span = int((end - start).total_seconds())
    for p in people:
        for _ in range(rng.randint(*per_person)):
            camp = rng.choice(CAMPAIGNS[partner])
            ts = start + timedelta(seconds=rng.randint(0, span))
            # marketing tools rarely normalise emails: mixed case and stray whitespace
            email = p["email"] if rng.random() > 0.15 else f" {p['email'].upper()} "
            base = {"partnerCode": partner, "campaignId": camp, "leadEmail": email}
            events.append({**base, "activityId": f"A{aid}", "activityType": "send",
                           "activityDate": ts.strftime("%Y-%m-%dT%H:%M:%SZ")})
            aid += 1
            if rng.random() < 0.35:
                events.append({**base, "activityId": f"A{aid}", "activityType": "open",
                               "activityDate": (ts + timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")})
                aid += 1
                if rng.random() < 0.3:
                    events.append({**base, "activityId": f"A{aid}", "activityType": "click",
                                   "activityDate": (ts + timedelta(hours=3, minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")})
                    aid += 1
            if rng.random() < 0.012:
                events.append({**base, "activityId": f"A{aid}", "activityType": "unsubscribe",
                               "activityDate": (ts + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ")})
                aid += 1
    return events, aid


def _epoch_ms(ts: datetime) -> str:
    return str(calendar.timegm(ts.timetuple()) * 1000)


TEALIUM_ACCOUNT = "travelrewards"
TEALIUM_PROFILE = {"BHB": "bhb-web", "SAM": "sam-web", "NWCU": "nwcu-web"}
WEB_PRODUCT_WEIGHTS = {"CRUISE": 0.35, "HOTEL": 0.35, "FLIGHT": 0.2, "CAR": 0.1}


def _tealium(partner, people, rng, start, end, visits, prefix, anonymous):
    """Tealium EventStream-style web events (what a webhook / S3 connector delivers).

    Members browse product pages; about half of their events are logged in (and carry
    customer_email). Some visitors decline personalization in Tealium's consent manager,
    and a few update email preferences through a Tealium-tagged preference center.
    """
    events = []
    span = int((end - start).total_seconds())
    prods, weights = list(WEB_PRODUCT_WEIGHTS), list(WEB_PRODUCT_WEIGHTS.values())

    def event(vid, name, ts, cats, email=None, **extra):
        e = {"tealium_event_id": f"{prefix}{len(events)}", "tealium_account": TEALIUM_ACCOUNT,
             "tealium_profile": TEALIUM_PROFILE[partner], "tealium_datasource": "web",
             "tealium_visitor_id": vid, "tealium_event": name, "tealium_timestamp_epoch": _epoch_ms(ts),
             "consent_categories": cats, **extra}
        if email:
            e["customer_email"] = email if rng.random() > 0.1 else f" {email.upper()} "
        events.append(e)

    for p in people:
        if rng.random() > 0.6:
            continue
        vid = f"{partner.lower()}-v{p['id']:06d}"
        cats = "analytics,personalization" if rng.random() > 0.2 else "analytics"
        for _ in range(rng.randint(*visits)):
            ts = start + timedelta(seconds=rng.randint(0, span))
            prod = rng.choices(prods, weights)[0]
            name = rng.choice(["product_view", "product_view", "product_view", "search", "booking_start"])
            event(vid, name, ts, cats, p["email"] if rng.random() < 0.5 else None,
                  page_type="product" if name == "product_view" else name, product_category=prod.lower())
        roll = rng.random()
        if roll < 0.02:
            event(vid, "email_preferences_update", start + timedelta(seconds=rng.randint(0, span)), cats,
                  p["email"], email_marketing_opt_in="false")
        elif roll < 0.06 and p["country"] in OPT_IN_COUNTRIES:
            event(vid, "email_preferences_update", start + timedelta(seconds=rng.randint(0, span)), cats,
                  p["email"], email_marketing_opt_in="true")
    for i in range(anonymous):
        ts = start + timedelta(seconds=rng.randint(0, span))
        prod = rng.choices(prods, weights)[0]
        event(f"{partner.lower()}-anon{prefix}{i}", "product_view", ts, "analytics,personalization",
              page_type="product", product_category=prod.lower())
    return events


def _consent_event(eid, partner, email, consent, ts, source):
    return {"event_id": eid, "partner_code": partner, "email": email, "channel": "email",
            "consent": consent, "source": source, "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ")}


OPT_IN_COUNTRIES = {"CA", "GB", "DE", "FR"}


def generate(day: int) -> dict:
    onprem = settings.SOURCES / "onprem"
    onprem.mkdir(parents=True, exist_ok=True)
    landing = settings.SOURCES / "landing"
    summary = {}
    all_engagement, consent, web = [], [], []
    aid = 1
    for partner in PARTNER_SHAPE:
        # one RNG per partner so day 2 replays day 1's state exactly
        rng = random.Random(f"{SEED}:{partner}")
        people = _people(partner, rng)
        bookings = _bookings(partner, people, rng, start_id=1)
        # --- deliberate data-quality issues -------------------------------------------
        if partner == "BHB":
            for p in people[10:16]:
                p["email"] = p["email"].split("@")[0] + "@example"  # malformed
            for b in bookings[20:23]:
                b["amount"] = -abs(b["amount"])  # negative
        if partner == "SAM":
            for b in bookings[40:44]:
                b["currency"] = "JPY"  # unsupported currency
        if partner == "NWCU":
            for p in people[5:7]:
                p["iso3"] = "XXX"  # unknown country

        db = onprem / f"{partner.lower()}.sqlite"
        mem_t, bk_t = TABLES[partner]
        if day == 1:
            if db.exists():
                db.unlink()
            conn = sqlite3.connect(db)
            for ddl in DDL[partner]:
                conn.execute(ddl)
            _upsert_rows(conn, mem_t, [_member_row(partner, p) for p in people if not p["late"]])
            _upsert_rows(conn, bk_t, [_booking_row(partner, b) for b in bookings])
            conn.commit()
            conn.close()
            summary[partner] = {"members": sum(not p["late"] for p in people), "bookings": len(bookings)}

            rng = random.Random(f"{SEED}:{partner}:events")
            ev, aid = _engagement(partner, [p for p in people if not p["late"]], rng,
                                  REF[1] - timedelta(days=200), REF[1] - timedelta(hours=4), (2, 6), aid)
            ev += [{"partnerCode": partner, "campaignId": CAMPAIGNS[partner][0],
                    "leadEmail": f"prospect{i}@example.com", "activityId": f"P{partner}{i}",
                    "activityType": "send", "activityDate": "2026-10-15T10:00:00Z"} for i in range(25)]
            all_engagement += ev
            trng = random.Random(f"{SEED}:{partner}:tealium:day1")
            web += _tealium(partner, [p for p in people if not p["late"]], trng,
                            REF[1] - timedelta(days=60), REF[1] - timedelta(hours=2), (1, 6), f"T1{partner}", 80)
            for p in people:
                if p["late"]:
                    continue
                ts = p["created"] + timedelta(days=1)
                if p["country"] in OPT_IN_COUNTRIES:
                    if rng.random() < 0.55:
                        consent.append(_consent_event(f"C{len(consent)}", partner, p["email"], "opt_in", ts, "signup_form"))
                    if rng.random() < 0.05:
                        consent.append(_consent_event(f"C{len(consent)}", partner, p["email"], "opt_out",
                                                      REF[1] - timedelta(days=rng.randint(1, 60)), "preference_center"))
                elif rng.random() < 0.07:
                    consent.append(_consent_event(f"C{len(consent)}", partner, p["email"], "opt_out",
                                                  REF[1] - timedelta(days=rng.randint(1, 300)), "preference_center"))
        else:  # ------------------------------------------------------ day 2 changes
            rng2 = random.Random(f"{SEED}:{partner}:day{day}")
            now = REF[2] - timedelta(hours=1)
            conn = sqlite3.connect(db)
            changed = []
            existing = [p for p in people if not p["late"]]
            tiers = PARTNER_SHAPE[partner][2]
            for p in rng2.sample(existing, k=len(existing) // 20):  # 5% tier change -> SCD2
                idx = tiers.index(p["tier"])
                p["tier"] = tiers[min(idx + 1, len(tiers) - 1)] if idx < len(tiers) - 1 else tiers[idx - 1]
                p["modified"] = now - timedelta(minutes=rng2.randint(1, 600))
                changed.append(p)
            for p in rng2.sample(existing, k=len(existing) // 50):  # 2% relocate
                p["state"] = rng2.choice([s for s in STATES[p["country"]] if s != p["state"]] or [p["state"]])
                p["modified"] = now - timedelta(minutes=rng2.randint(1, 600))
                changed.append(p)
            late = [p for p in people if p["late"]]
            for p in late:
                p["modified"] = now - timedelta(minutes=30)
            new_people = []
            for j in range(len(existing) // 33):  # ~3% brand-new members
                i = len(people) + j + 1
                fn, ln = rng2.choice(FIRST), rng2.choice(LAST)
                c = rng2.choice(list(PARTNER_SHAPE[partner][1]))
                new_people.append({"id": i, "first": fn, "last": ln,
                                   "email": f"{fn.lower()}.{ln.lower()}{i}@example.com",
                                   "phone": f"+1-555-01{rng2.randint(10, 99)}-{rng2.randint(1000, 9999)}",
                                   "country": c, "state": rng2.choice(STATES[c]), "tier": tiers[0],
                                   "created": now - timedelta(hours=5), "modified": now - timedelta(hours=5),
                                   "late": False})
            _upsert_rows(conn, mem_t, [_member_row(partner, p) for p in changed + late + new_people])

            new_b = _bookings(partner, rng2.sample(existing, k=len(existing) // 4) + new_people, rng2,
                              start_id=len(bookings) + 1, n_per_person=(1, 1),
                              window=(REF[1], now))
            cancels = [b for b in bookings if b["status"] == "CONFIRMED"]
            cancels = rng2.sample(cancels, k=len(cancels) // 30)
            for b in cancels:
                b["status"] = "CANCELLED"
                b["modified"] = now - timedelta(minutes=rng2.randint(1, 300))
            fixes = []
            if partner == "BHB":  # source team corrects one of the negative amounts
                b = bookings[20]
                b["amount"] = abs(b["amount"])
                b["modified"] = now - timedelta(minutes=45)
                fixes.append(b)
            _upsert_rows(conn, bk_t, [_booking_row(partner, b) for b in new_b + cancels + fixes])
            conn.commit()
            conn.close()
            summary[partner] = {"changed_members": len(changed), "late_members": len(late),
                                "new_members": len(new_people), "new_bookings": len(new_b),
                                "cancellations": len(cancels), "source_fixes": len(fixes)}

            ev, aid = _engagement(partner, rng2.sample(existing, k=len(existing) // 3), rng2,
                                  REF[1], now, (1, 1), 10_000_000 + aid)
            all_engagement += ev
            trng = random.Random(f"{SEED}:{partner}:tealium:day2")
            web += _tealium(partner, existing, trng, REF[1], now, (0, 2), f"T2{partner}", 20)
            # consent: a wave of opt-outs, one out-of-order late event, one same-ts conflict
            for p in rng2.sample(existing, k=12):
                consent.append(_consent_event(f"D{partner}{p['id']}", partner, p["email"], "opt_out",
                                              now - timedelta(minutes=rng2.randint(1, 300)), "preference_center"))
            target = existing[0]
            consent.append(_consent_event(f"LATE{partner}", partner, target["email"], "opt_in",
                                          REF[1] - timedelta(days=400), "legacy_crm_backfill"))
            consent.append(_consent_event(f"OUT{partner}", partner, target["email"], "opt_out",
                                          now - timedelta(minutes=10), "preference_center"))
            tie = existing[1]
            tie_ts = now - timedelta(minutes=5)
            consent.append(_consent_event(f"TIEIN{partner}", partner, tie["email"], "opt_in", tie_ts, "signup_form"))
            consent.append(_consent_event(f"TIEOUT{partner}", partner, tie["email"], "opt_out", tie_ts, "call_center"))

    _jsonl(landing / "marketing" / f"engagement_day{day}.jsonl", all_engagement)
    _jsonl(landing / "tealium" / f"eventstream_day{day}.jsonl", web)
    # the consent stream arrives as several micro-batch files
    for k in range(0, len(consent), 400):
        _jsonl(landing / "consent_events" / f"consent_day{day}_{k // 400:03d}.json", consent[k:k + 400])
    settings.write_state("run_context", {"day": day, "as_of": REF[day].isoformat()})
    summary["engagement_events"] = len(all_engagement)
    summary["consent_events"] = len(consent)
    summary["tealium_events"] = len(web)
    return summary


def main():
    ap = argparse.ArgumentParser(description="Generate synthetic partner source systems")
    ap.add_argument("--day", type=int, choices=[1, 2], required=True)
    args = ap.parse_args()
    print(json.dumps(generate(args.day), indent=2))


if __name__ == "__main__":
    main()


def as_of() -> date:
    ctx = settings.read_state("run_context", {"as_of": REF[1].isoformat()})
    return datetime.fromisoformat(ctx["as_of"]).date()
