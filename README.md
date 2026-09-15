# Submission 2 — Reproducible NYC TLC Data Pipeline

Repository นี้พัฒนาจาก Submission 1 เพื่อสร้าง **Data Pipeline ที่รันซ้ำได้ ตรวจสอบคุณภาพได้ และอัปเดตช่วงเวลาใหม่ได้** สำหรับคำถามเดิม:

> **Which taxi type was the most used in the first half of 2024?**

คำตอบที่ pipeline รักษาไว้เหมือน baseline คือ **High Volume FHV (HVFHV)** ด้วย 120,864,668 valid trip records หรือ 80.7618% ของระเบียนที่ผ่านเกณฑ์ทั้งหมด 149,655,751 รายการ ผลนี้หมายถึงจำนวน trip-record rows ที่รายงานในข้อมูล TLC ไม่ใช่จำนวนผู้โดยสารที่ไม่ซ้ำกันหรือจำนวนรถ

## สิ่งที่พัฒนาจาก Submission 1

Baseline เป็นการวิเคราะห์แบบ Notebook ที่รวมการดาวน์โหลด การตรวจข้อมูล และการนำเสนอไว้ในลำดับเซลล์เดียวกัน ส่วน Submission 2 แยกงานเป็นขั้นตอนที่มีสัญญาและผลลัพธ์ชัดเจน ได้แก่ source manifest, acquisition/cache, inspection, curation, quality gates, provenance, run manifest และ report outputs การเปลี่ยนแปลงนี้ทำให้ผู้ตรวจสามารถรันเฉพาะช่วงเดือนที่ต้องการซ้ำได้ และสามารถเพิ่มเดือน/ปีใหม่ด้วยพารามิเตอร์โดยไม่ต้องแก้ logic หลัก

| ประเด็น | Submission 1 baseline | Submission 2 pipeline |
| --- | --- | --- |
| วิธีรัน | Notebook ตามลำดับเซลล์ | CLI script ที่มี parameters และ exit status |
| แหล่งข้อมูล | URL ที่ฝังใน Notebook | source manifest และ source registry ที่เก็บ HTML/PDF/Parquet provenance |
| การประมวลผล | ตรวจและรวมใน Notebook | stages แยกชัดเจน: acquire → inspect → quality → curate → report |
| การอัปเดต | ต้องแก้/รัน Notebook ใหม่ | `--year`, `--start-month`, `--end-month` รองรับช่วงใหม่ |
| การตรวจคุณภาพ | assertions บางส่วน | quality report JSON และ quality gates ที่ทำให้ run ล้มเหลวเมื่อไม่ผ่าน |
| หลักฐานการรัน | output ใน Notebook | `runs/` มี JSON status, latest manifest และ log |
| ผลลัพธ์ | ตารางและกราฟ | curated contract, CSV/JSON reports และ sample outputs |

## แหล่งข้อมูลและรูปแบบข้อมูล

Pipeline เชื่อมโยงข้อมูลหลายรูปแบบจากแหล่งทางการของ NYC Taxi & Limousine Commission (TLC) ได้แก่หน้า catalog แบบ HTML, data dictionaries แบบ PDF และไฟล์ trip records แบบ Parquet [1] [2] [3] ไฟล์ Parquet มี schema ต่างกันตามบริการ จึงเลือกใช้ schema กลางขั้นต่ำที่ประกอบด้วยประเภทบริการ เดือน จำนวนแถว เวลา pickup และ valid trip count

| Source | รูปแบบ | ใช้ทำอะไร |
| --- | --- | --- |
| TLC Trip Record Data page | HTML / URL catalog | ยืนยันชุดข้อมูลและบริบทการเผยแพร่ |
| Yellow Taxi dictionary | PDF | ยืนยันความหมายของ `tpep_pickup_datetime` |
| HVFHV dictionary | PDF | ยืนยันความหมายของ `pickup_datetime` และ trip row |
| Yellow/Green/FHV/HVFHV monthly records | Parquet | ข้อมูลหลักสำหรับนับ trip records |
| `config/source_registry.json` | JSON | ลงทะเบียนแหล่งข้อมูลและ curated contract |
| `config/source_manifest.csv` | CSV | รายการไฟล์ที่ pipeline ต้องใช้ในแต่ละ run |

## โครงสร้าง Repository

```text
.
├── config/
│   ├── source_registry.json       # แหล่งข้อมูลหลายรูปแบบและ curated contract
│   └── source_manifest.csv        # manifest ที่สร้างจาก parameters ของ run
├── pipeline/
│   ├── nyc_tlc_pipeline.py        # source, transform, quality, curation, reports
│   └── requirements.txt
├── scripts/
│   └── run_pipeline.py            # CLI entry point
├── tests/
│   └── test_pipeline.py           # automated tests ของ contract และ quality gates
├── data/
│   ├── raw/                       # local cache; ไม่ commit Parquet ขนาดใหญ่
│   └── curated/                   # curated outputs ที่พร้อมวิเคราะห์ต่อ
├── reports/
│   ├── quality_report.json        # PASS/FAIL และ checks รายข้อ
│   └── analysis_metrics.json      # คำตอบหลักและ metrics
├── runs/
│   ├── latest.json                # สถานะ run ล่าสุด
│   └── pipeline_*.log             # หลักฐาน log ราย run
├── docs/data_flow.mmd             # Data Flow Diagram
├── docs/design_decisions.md       # การตัดสินใจเชิงสถาปัตยกรรม
├── docs/learning_reflection.md   # reflection 3–5 ประโยค
└── sample/                        # curated sample สำหรับผู้ตรวจ
```

ไฟล์ Parquet ดิบไม่ถูก commit เข้า repository เพราะมีขนาดประมาณ 3.35 GB สำหรับ H1 2024 ผู้ตรวจสามารถดาวน์โหลดใหม่ด้วย `--download` จาก URL ทางการ และสามารถใช้ไฟล์ใน `sample/` เพื่อตรวจโครงสร้าง output โดยไม่ต้องเก็บข้อมูลดิบไว้ใน Git

## ติดตั้ง

ต้องใช้ Python 3.10 ขึ้นไปและอินเทอร์เน็ตเมื่อดาวน์โหลดข้อมูลจริง

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r pipeline/requirements.txt
```

หากต้องการตรวจเฉพาะโค้ดและไม่ต้องดาวน์โหลดข้อมูล ให้รัน tests ได้ทันทีหลังติดตั้ง dependencies

```bash
PYTHONPATH=. pytest -q
```

## วิธีรัน Pipeline

### Full run สำหรับคำถามเดิม

คำสั่งนี้สร้าง manifest สำหรับ 4 service types × 6 เดือน ตรวจว่ามีไฟล์อยู่ใน cache ถ้าไม่มีจะดาวน์โหลด จากนั้นประมวลผลและเขียน curated outputs กับ reports

```bash
python scripts/run_pipeline.py --year 2024 --start-month 1 --end-month 6 --download --force
```

หากดาวน์โหลดไฟล์ไว้แล้ว สามารถตัด `--download` ออกได้ pipeline จะใช้ local cache และไม่ดาวน์โหลดซ้ำ

### Incremental-compatible run

Pipeline ใช้ช่วงเดือนเป็น parameter จึงรองรับการประมวลผลเดือนใหม่หรือช่วงใหม่โดยไม่แก้ source code เช่น

```bash
python scripts/run_pipeline.py --year 2024 --start-month 7 --end-month 9 --download
```

คำสั่งนี้สร้าง manifest และ outputs สำหรับ July–September 2024 แยกตาม run configuration การออกแบบนี้เป็น incremental-compatible: ไฟล์ที่มีอยู่จะถูกใช้ซ้ำ และไฟล์ใหม่จะถูกดึงมาเฉพาะเมื่อไม่พบใน `data/raw/`

### ตรวจสอบสถานะการทำงาน

เมื่อ run สำเร็จ ให้ตรวจไฟล์ต่อไปนี้

| ไฟล์ | สิ่งที่ตรวจสอบ |
| --- | --- |
| `runs/latest.json` | status, parameters, run id และ winner |
| `runs/pipeline_2024_01_06.log` | ลำดับขั้นตอนและ error/warning |
| `reports/quality_report.json` | quality gates ทุกข้อและสถานะ PASS/FAIL |
| `reports/analysis_metrics.json` | ผลวิเคราะห์และจำนวนแถวทั้งหมด |
| `data/curated/half_year_usage.csv` | ตารางสรุปที่พร้อมวิเคราะห์ต่อ |

ถ้า quality gate สำคัญไม่ผ่าน pipeline จะเขียน status `FAILED` และหยุดก่อนสร้างผลลัพธ์ที่อาจทำให้เข้าใจผิด

## Curated output contract

ระดับ curated มี grain เป็น **หนึ่งแถวต่อ service type และ calendar month** สำหรับ `monthly_usage.csv` และหนึ่งแถวต่อ service type สำหรับ `half_year_usage.csv`

| Output | Grain | ใช้ทำอะไร |
| --- | --- | --- |
| `source_inventory.csv` | หนึ่งแถวต่อไฟล์ต้นทาง | provenance, byte size, SHA-256, schema fingerprint |
| `monthly_usage.csv` | service × month | ตรวจคุณภาพและสร้าง trend |
| `half_year_usage.csv` | service | ranking และ share ของ most used |
| `yellow_green_sensitivity.csv` | Yellow/Green service | ตรวจการตีความคำว่า taxi แบบแคบ |
| `service_schema_map.csv` | service | mapping ของ pickup column ที่ใช้ |

คอลัมน์สำคัญของ `monthly_usage.csv` ได้แก่ `raw_rows`, `pickup_nulls`, `out_of_window`, `excluded_records` และ `valid_trip_records` โดย `valid_trip_records` นับเฉพาะแถวที่ pickup timestamp ไม่เป็น null และอยู่ในช่วง `[period_start, period_end_exclusive)`

## Quality gates

Pipeline ตรวจคุณภาพก่อนคำนวณอันดับ โดยตรวจจำนวนไฟล์ตาม manifest, ความสอดคล้องระหว่าง Parquet metadata กับจำนวนแถวที่ query ได้, การมี pickup column, จำนวนที่ไม่ติดลบ, valid count ไม่เกิน raw count และจำนวน pickup null ใน curated output หากข้อใดไม่ผ่าน `quality_report.json` จะมี `status: FAIL` พร้อมชื่อ checks ที่ล้มเหลว และ pipeline จะหยุดด้วย error

สำหรับ H1 2024 baseline ที่ตรวจสอบแล้ว raw rows มี 149,655,796 แถว, pickup null 0 แถว, pickup นอกช่วง 45 แถว และ valid trip records 149,655,751 แถว

## Reproducibility และ provenance

ทุก source file มี URL, local path, byte size, SHA-256, Parquet row groups, metadata row count และ schema fingerprint ใน `data/curated/source_inventory.csv` การอ่าน Parquet ใช้ DuckDB แบบ columnar และไม่โหลดทุกคอลัมน์เป็น DataFrame ขนาดใหญ่ก่อนการนับ ทำให้ workflow เหมาะกับข้อมูลหลาย GB มากกว่าแนวทางที่อ่านทุกแถวเข้าหน่วยความจำ

Run parameters ถูกเก็บใน `runs/run_<year>_<start>_<end>.json` จึงสามารถตอบได้ว่า output เกิดจากช่วงเวลาใด ดาวน์โหลดหรือใช้ cache หรือไม่ และจบด้วยสถานะอะไร

## ข้อจำกัดปัจจุบัน

การอัปเดตเป็นแบบ incremental-compatible แต่ยัง recompute curated summary ของช่วงที่สั่งทั้งหมดทุกครั้ง ไม่ได้ใช้ฐานข้อมูลหรือ state store สำหรับ merge partition แบบถาวร องค์ประกอบ source catalog และ data dictionaries ถูกเก็บเป็น metadata registry แต่ pipeline ประมวลผลตัวเลขจาก Parquet เท่านั้น นอกจากนี้ TLC ระบุว่าข้อมูลมาจากผู้ให้บริการและไม่ได้รับรองความครบถ้วนโดยสมบูรณ์ ผลลัพธ์จึงควรตีความเป็นจำนวน trip records ที่ถูกรายงานในชุดข้อมูล ไม่ใช่จำนวนการเดินทางจริงทั้งหมด [1]

## Data Flow Diagram

ดู `docs/data_flow.mmd` หรือ render ด้วย Mermaid-compatible viewer

## References

[1]: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

[2]: [NYC TLC Yellow Taxi Trip Records Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

[3]: [NYC TLC High Volume FHV Trip Records Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf)
