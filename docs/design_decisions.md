# Design Decisions — Submission 2

## Decision 1: ใช้ manifest เป็นตัวควบคุม source acquisition

Pipeline ไม่ hard-code รายการไฟล์ไว้ในขั้น transform แต่สร้าง `config/source_manifest.csv` จากปีและช่วงเดือนที่ผู้ใช้ระบุ แล้วใช้ manifest เป็นรายการที่ต้องมีและตรวจสอบ ผลดีคือผู้ตรวจเห็น URL, service type, month และ data dictionary ที่ผูกกับแต่ละ source อย่างชัดเจน และการเพิ่มเดือนใหม่ทำได้ด้วย parameter โดยไม่แก้ logic การประมวลผล

## Decision 2: แยก source cache ออกจาก curated output

ไฟล์ Parquet ดิบเก็บใน `data/raw/` และผลที่ผ่านการคัดกรองเก็บใน `data/curated/` คนละชั้นกัน เพราะ raw ต้องรักษาความสามารถในการตรวจย้อนกลับ ส่วน curated ต้องมี schema ที่สม่ำเสมอและพร้อมนำไปวิเคราะห์ต่อ การแยกชั้นยังทำให้ไม่ต้อง commit ไฟล์หลาย GB เข้า Git repository

## Decision 3: ใช้ schema กลางขั้นต่ำแทนการบังคับรวมทุกคอลัมน์

Yellow, Green, FHV และ HVFHV มี schema และชื่อ pickup column ต่างกัน Pipeline จึง map ไปยัง `service_key`, `service_type`, `year`, `month`, `pickup_column`, `raw_rows`, quality counts และ `valid_trip_records` เท่านั้น เหตุผลคือคำถามต้องการเปรียบเทียบความถี่ของ trip records ไม่ได้ต้องการค่าโดยสารหรือรายละเอียดทุกฟิลด์ การลด schema ทำให้การบูรณาการมีความหมายและไม่สร้างความเทียบเทียมปลอม

## Decision 4: Quality gates ต้องหยุด pipeline เมื่อผลอาจไม่น่าเชื่อถือ

การตรวจ metadata row count, pickup column, valid count และ null count ถูกทำก่อน curation หาก gate สำคัญไม่ผ่าน pipeline จะเขียน quality report และ run status เป็น FAILED แทนการสร้าง output ที่ดูเหมือนสำเร็จ การออกแบบนี้ให้ความสำคัญกับความถูกต้องและตรวจสอบได้มากกว่าการพยายามสร้างผลลัพธ์ให้เสร็จทุกกรณี

## Decision 5: ใช้ไฟล์ JSON/CSV เป็น evidence contract

สถานะ run, metrics, quality report และ curated data ถูกเขียนเป็น JSON/CSV ที่อ่านได้ด้วยเครื่องมือทั่วไป แทนการเก็บเฉพาะข้อความในหน้าจอหรือ Notebook เพราะผู้ตรวจสามารถเปิดไฟล์ ตรวจค่า และนำ curated data ไปวิเคราะห์ต่อได้โดยไม่ต้อง rerun ทุกขั้นตอน
