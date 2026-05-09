# GB Bank Holiday Calendar 2010–2045

## File
holiday_calendar_2010_2045.csv

## Columns
| Column                | Type    | Description |
|-----------------------|---------|-------------|
| date                  | date    | One row per calendar day, 2010-01-01 to 2045-12-31 |
| is_holiday_eng_wales  | int 0/1 | 1 if a bank holiday in England & Wales |
| is_holiday_scotland   | int 0/1 | 1 if a bank holiday in Scotland |
| is_holiday_gb_any     | int 0/1 | 1 if a bank holiday in either England & Wales or Scotland |

Northern Ireland is excluded entirely — not loaded, not used.

---

## Sources and methodology

### 2019–2028 — trusted source
GOV.UK bank-holidays.json (https://www.gov.uk/bank-holidays.json).
England & Wales and Scotland divisions used directly as the authoritative source.
Northern Ireland division was discarded at load time.

### 2010–2018 and 2029–2045 — algorithmic
Bank holidays computed using standard UK rules.

**England & Wales rules**
- New Year's Day (1 Jan, with weekend substitute)
- Good Friday (Easter − 2 days)
- Easter Monday (Easter + 1 day)
- Early May bank holiday (first Monday in May)
- Spring bank holiday (last Monday in May)
- Summer bank holiday (last Monday in August)
- Christmas Day (25 Dec, with weekend substitute)
- Boxing Day (26 Dec, with weekend substitute)

**Scotland differences from E&W**
- 2nd January added (with weekend substitute)
- Easter Monday not observed
- Summer bank holiday = first Monday in August (not last)
- St Andrew's Day added (30 Nov, with weekend substitute)

Easter dates use the Meeus/Jones/Butcher anonymous Gregorian algorithm.

---

## Known one-off special holidays hardcoded (2010–2028)
The following non-standard bank holidays cannot be derived algorithmically
and are hardcoded for the period outside JSON coverage:

| Date       | Description                                | Nations        |
|------------|--------------------------------------------|----------------|
| 2011-04-29 | Royal Wedding (Prince William & Catherine) | E&W, Scotland  |
| 2012-06-04 | Diamond Jubilee — Spring BH moved to Jun 4 | E&W, Scotland  |
| 2012-06-05 | Diamond Jubilee — extra bank holiday       | E&W, Scotland  |
| 2020-05-08 | VE Day 75 — Early May BH moved from May 4  | E&W, Scotland  |
| 2022-06-02 | Platinum Jubilee — Spring BH moved to Jun 2| E&W, Scotland  |
| 2022-06-03 | Platinum Jubilee — extra bank holiday      | E&W, Scotland  |
| 2022-09-19 | State Funeral of Queen Elizabeth II        | E&W, Scotland  |
| 2023-05-08 | Coronation of King Charles III             | E&W, Scotland  |

---

## Limitation for 2029–2045
Future special or one-off bank holidays (royal events, jubilees, national
occasions) cannot be known in advance and are NOT included for 2029–2045.
This calendar should be updated manually when such dates are announced.

---

## Verification
The algorithm was validated against the GOV.UK JSON for the overlapping
years 2019–2028. All dates matched exactly — zero discrepancies for both
England & Wales and Scotland.

---

## Quality checks
| Check                  | Result                        |
|------------------------|-------------------------------|
| Total rows             | 13,149 (expected 13,149) |
| Duplicate dates        | 0                            |
| Missing values         | 0                            |
| E&W holidays           | 293                           |
| Scotland holidays      | 329                           |
| GB any holidays        | 401                           |
