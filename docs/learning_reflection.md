# Learning Reflection

การเปลี่ยนจาก Notebook เป็น pipeline ทำให้เห็นว่าการตอบคำถามเชิงวิเคราะห์ต้องแยกการได้มาของข้อมูล การตรวจคุณภาพ และการสร้างผลลัพธ์ออกจากกันอย่างชัดเจน การใช้ manifest และ checksum ช่วยให้ระบุได้ว่า output เกิดจากไฟล์ใดและทำซ้ำได้อย่างไร Quality gates ทำให้ความผิดพลาดของ schema หรือจำนวนแถวถูกตรวจพบก่อนส่งต่อไปยัง curated dataset สิ่งที่ยังต้องพัฒนาคือการเพิ่ม persistent state สำหรับ merge เฉพาะ partition ใหม่ และการเพิ่มการตรวจคุณภาพเชิงธุรกิจ เช่น ระยะทางและค่าโดยสารที่ผิดปกติ

## Current limitations

Pipeline รุ่นนี้รองรับการรันช่วงเดือนใหม่ด้วย parameters และ reuse ไฟล์ใน local cache แต่ยัง recompute summary ของช่วงที่สั่งทั้งหมดทุกครั้ง ไม่มี orchestration service หรือฐานข้อมูลสำหรับเก็บ state ระยะยาว และยังไม่ทำ deduplication หรือ anomaly detection ของทุกฟิลด์ใน trip records
