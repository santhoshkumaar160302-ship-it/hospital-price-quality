"""Generates small, deterministic CSVs with the same headers as the real CMS files.

Used by unit tests, the offline demo and CI. Two phases simulate two quarterly
releases, so tests can check snapshots (SCD2), incremental loads and schema drift.

Edge cases built in on purpose (all seen in real CMS data):
  - CCN "50001" with its leading zero stripped (should become "050001")
  - "Not Available" ratings and scores, "Number of Cases Too Small"
  - 2023 inpatient file uses the old column name Rndrng_CCN
  - a closed hospital that bills Medicare but isn't in Hospital General Information
  - phase 2 adds a brand-new column to Hospital General Information
  - phase 2 changes one hospital's star rating and another's ownership

Run: python tests/fixtures/generate_fixtures.py
"""

import csv
import random
from pathlib import Path

OUT = Path(__file__).resolve().parent
NA = "Not Available"

HOSPITALS = [
    # ccn, name, city, state, zip, county, type, ownership, emergency, rating, price factor
    ("150001", "RIVERBEND REGIONAL MEDICAL CENTER", "BLOOMINGTON", "IN", "47401", "MONROE",
     "Acute Care Hospitals", "Voluntary non-profit - Private", "Yes", "4", 0.92),
    ("150002", "HOOSIER VALLEY HOSPITAL", "INDIANAPOLIS", "IN", "46202", "MARION",
     "Acute Care Hospitals", "Proprietary", "Yes", "2", 1.18),
    ("150003", "PRAIRIE CREEK COMMUNITY HOSPITAL", "LAFAYETTE", "IN", "47904", "TIPPECANOE",
     "Acute Care Hospitals", "Government - Hospital District or Authority", "Yes", "3", 0.97),
    ("150004", "LIMESTONE CRITICAL ACCESS HOSPITAL", "BEDFORD", "IN", "47421", "LAWRENCE",
     "Critical Access Hospitals", "Government - Local", "Yes", NA, 1.05),
    ("50001", "PACIFIC CREST MEDICAL CENTER", "SAN JOSE", "CA", "95112", "SANTA CLARA",
     "Acute Care Hospitals", "Voluntary non-profit - Private", "Yes", "5", 1.31),
    ("050002", "GOLDEN HILLS HOSPITAL", "SACRAMENTO", "CA", "95816", "SACRAMENTO",
     "Acute Care Hospitals", "Voluntary non-profit - Private", "Yes", "3", 1.22),
    ("050003", "BAYVIEW GENERAL HOSPITAL", "OAKLAND", "CA", "94609", "ALAMEDA",
     "Acute Care Hospitals", "Proprietary", "Yes", "2", 1.58),
    ("050004", "SIERRA VISTA MEDICAL CENTER", "FRESNO", "CA", "93721", "FRESNO",
     "Acute Care Hospitals", "Government - Hospital District or Authority", "Yes", "4", 1.12),
    ("450001", "LONE STAR MEDICAL CENTER", "HOUSTON", "TX", "77030", "HARRIS",
     "Acute Care Hospitals", "Voluntary non-profit - Private", "Yes", "4", 1.08),
    ("450002", "BRAZOS RIVER HOSPITAL", "WACO", "TX", "76708", "MCLENNAN",
     "Acute Care Hospitals", "Proprietary", "Yes", "3", 0.94),
    ("450003", "GULF COAST REGIONAL HOSPITAL", "CORPUS CHRISTI", "TX", "78405", "NUECES",
     "Acute Care Hospitals", "Proprietary", "Yes", "1", 1.21),
    ("450004", "HILL COUNTRY MEDICAL CENTER", "AUSTIN", "TX", "78705", "TRAVIS",
     "Acute Care Hospitals", "Voluntary non-profit - Private", "Yes", "5", 0.99),
]
NEW_IN_PHASE2 = ("450005", "PANHANDLE PLAINS HOSPITAL", "AMARILLO", "TX", "79106", "POTTER",
                 "Acute Care Hospitals", "Government - Hospital District or Authority", "Yes", "3", 0.9)
CLOSED_HOSPITAL = ("159999", "FORMER EASTSIDE HOSPITAL", "EVANSVILLE", "IN", "47714", "VANDERBURGH",
                   "Acute Care Hospitals", "Proprietary", "No", NA, 1.0)

STATE_FACTOR = {"IN": 0.95, "CA": 1.30, "TX": 1.0}
STATE_FIPS = {"IN": "18", "CA": "06", "TX": "48"}
DRGS = {
    "470": ("MAJOR HIP AND KNEE JOINT REPLACEMENT OR REATTACHMENT OF LOWER EXTREMITY WITHOUT MCC", 15200),
    "291": ("HEART FAILURE AND SHOCK WITH MCC", 11400),
    "871": ("SEPTICEMIA OR SEVERE SEPSIS WITHOUT MV >96 HOURS WITH MCC", 17300),
}

GENERAL_HEADER = [
    "Facility ID", "Facility Name", "Address", "City/Town", "State", "ZIP Code", "County/Parish",
    "Telephone Number", "Hospital Type", "Hospital Ownership", "Emergency Services",
    "Meets criteria for birthing friendly designation", "Hospital overall rating",
    "Hospital overall rating footnote", "MORT Group Measure Count", "Count of Facility MORT Measures",
    "Count of MORT Measures Better", "Count of MORT Measures No Different", "Count of MORT Measures Worse",
    "MORT Group Footnote",
]
MEASURE_HEADER = [
    "Facility ID", "Facility Name", "Address", "City/Town", "State", "ZIP Code", "County/Parish",
    "Telephone Number", "Measure ID", "Measure Name", "Compared to National", "Denominator", "Score",
    "Lower Estimate", "Higher Estimate",
]
MEASURE_TAIL = ["Footnote", "Start Date", "End Date"]
UNPLANNED_HEADER = MEASURE_HEADER + ["Number of Patients", "Number of Patients Returned"] + MEASURE_TAIL
COMPLICATIONS_HEADER = MEASURE_HEADER + MEASURE_TAIL

UNPLANNED_MEASURES = [  # id, name, national value
    ("READM_30_HOSP_WIDE", "Hospital-Wide All-Cause Unplanned Readmission", 14.6),
    ("READM_30_HF", "Heart failure (HF) 30-Day Readmission Rate", 20.1),
    ("EDAC_30_HF", "Hospital return days for heart failure patients", 8.0),
]
COMPLICATION_MEASURES = [
    ("MORT_30_AMI", "Death rate for heart attack patients", 12.6),
    ("MORT_30_HF", "Death rate for heart failure patients", 11.3),
    ("MORT_30_PN", "Death rate for pneumonia patients", 16.4),
    ("COMP_HIP_KNEE", "Rate of complications for hip/knee replacement patients", 2.5),
    ("PSI_90", "CMS Medicare PSI 90: Patient safety and adverse events composite", 1.0),
]


def write(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def address(rng, h):
    return [f"{rng.randint(100, 9999)} MAIN STREET", h[2], h[3], h[4], h[5], f"({rng.randint(200, 999)}) 555-0100"]


def quality_factor(h) -> float:
    """Better-rated hospitals get better (lower) measure scores."""
    rating = h[9]
    return {"5": 0.85, "4": 0.93, "3": 1.0, "2": 1.08, "1": 1.15}.get(rating, 1.0)


def compared(score: float, national: float, value_word: str = "Rate") -> str:
    if score < national * 0.92:
        return f"Better Than the National {value_word}"
    if score > national * 1.08:
        return f"Worse Than the National {value_word}"
    return f"No Different Than the National {value_word}"


def general_rows(rng, hospitals, extra_column=False):
    rows = []
    for h in hospitals:
        better, worse = (1, 0) if h[9] in ("4", "5") else ((0, 1) if h[9] in ("1", "2") else (0, 0))
        row = [h[0], h[1], *address(rng, h), h[6], h[7], h[8], "Y" if h[9] != NA else "",
               h[9], "" if h[9] != NA else "16", "7", "7", str(better), str(7 - better - worse), str(worse), ""]
        if extra_column:
            row.append(rng.choice(["Yes", "No"]))
        rows.append(row)
    return rows


def measure_rows(rng, hospitals, measures, unplanned: bool):
    rows = []
    for h in hospitals:
        small = h[6] == "Critical Access Hospitals"
        for mid, name, national in measures:
            if small:
                vals = ["Number of Cases Too Small", NA, NA, NA, NA]
                patients = [NA, NA] if unplanned else []
                footnote = "1"
            else:
                denom = rng.randint(150, 2500)
                score = round(national * quality_factor(h) * rng.uniform(0.95, 1.05), 2 if mid == "PSI_90" else 1)
                word = "Value" if mid == "PSI_90" else ("Days" if mid.startswith("EDAC") else "Rate")
                vals = [compared(score, national, word), str(denom), str(score),
                        str(round(score * 0.85, 2)), str(round(score * 1.15, 2))]
                patients = [str(denom), str(int(denom * score / 100))] if unplanned else []
                footnote = ""
            rows.append([h[0], h[1], *address(rng, h), mid, name, *vals, *patients,
                         footnote, "07/01/2021", "06/30/2024"])
    return rows


def inpatient_rows(rng, hospitals, year_factor: float):
    rows = []
    for h in hospitals:
        ccn = h[0].zfill(6)
        drgs = ["291"] if h[6] == "Critical Access Hospitals" else list(DRGS)
        for drg in drgs:
            desc, base = DRGS[drg]
            total = base * STATE_FACTOR[h[3]] * h[10] * year_factor * rng.uniform(0.97, 1.03)
            rows.append([
                ccn, h[1].title(), h[2].title(), f"{rng.randint(100, 9999)} Main Street", STATE_FIPS[h[3]],
                h[4], h[3], "1", "Metropolitan area core: primary flow within an urbanized area of 50,000 and greater",
                drg, desc, str(rng.randint(11, 400)), f"{total * 3.8:.2f}", f"{total:.2f}", f"{total * 0.84:.2f}",
            ])
    return rows


INPATIENT_TAIL = ["Rndrng_Prvdr_Org_Name", "Rndrng_Prvdr_City", "Rndrng_Prvdr_St", "Rndrng_Prvdr_State_FIPS",
                  "Rndrng_Prvdr_Zip5", "Rndrng_Prvdr_State_Abrvtn", "Rndrng_Prvdr_RUCA", "Rndrng_Prvdr_RUCA_Desc",
                  "DRG_Cd", "DRG_Desc", "Tot_Dschrgs", "Avg_Submtd_Cvrd_Chrg", "Avg_Tot_Pymt_Amt",
                  "Avg_Mdcr_Pymt_Amt"]


def main() -> None:
    rng = random.Random(7)

    # ---- phase 1: first quarterly release + 2023 inpatient data ----
    p1 = OUT / "phase1"
    write(p1 / "hospital_general" / "2026-07-22.csv", GENERAL_HEADER, general_rows(rng, HOSPITALS))
    write(p1 / "unplanned_visits" / "2026-07-22.csv", UNPLANNED_HEADER,
          measure_rows(rng, HOSPITALS, UNPLANNED_MEASURES, unplanned=True))
    write(p1 / "complications_deaths" / "2026-07-22.csv", COMPLICATIONS_HEADER,
          measure_rows(rng, HOSPITALS, COMPLICATION_MEASURES, unplanned=False))
    write(p1 / "inpatient_drg" / "2023.csv", ["Rndrng_CCN", *INPATIENT_TAIL],  # old column name
          inpatient_rows(rng, HOSPITALS + [CLOSED_HOSPITAL], year_factor=1.0))

    # ---- phase 2: next quarterly release + 2024 inpatient data ----
    phase2 = []
    for h in HOSPITALS:
        if h[0] == "050003":
            h = h[:9] + ("3",) + h[10:]                                  # rating improved 2 -> 3
        if h[0] == "150002":
            h = h[:7] + ("Voluntary non-profit - Private",) + h[8:]       # hospital acquired
        phase2.append(h)
    phase2.append(NEW_IN_PHASE2)
    p2 = OUT / "phase2"
    write(p2 / "hospital_general" / "2026-10-22.csv", GENERAL_HEADER + ["Telehealth Services"],  # new column
          general_rows(rng, phase2, extra_column=True))
    write(p2 / "unplanned_visits" / "2026-10-22.csv", UNPLANNED_HEADER,
          measure_rows(rng, phase2, UNPLANNED_MEASURES, unplanned=True))
    write(p2 / "complications_deaths" / "2026-10-22.csv", COMPLICATIONS_HEADER,
          measure_rows(rng, phase2, COMPLICATION_MEASURES, unplanned=False))
    write(p2 / "inpatient_drg" / "2024.csv", ["Rndrng_Prvdr_CCN", *INPATIENT_TAIL],
          inpatient_rows(rng, phase2 + [CLOSED_HOSPITAL], year_factor=1.04))
    print(f"fixtures written to {OUT}")


if __name__ == "__main__":
    main()
