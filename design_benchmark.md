# Database CHALLENGE

**Mục tiêu:** Xây dựng, Benchmark và So sánh kiến trúc Database giữa RDBMS (MySQL/PostgreSQL) và NoSQL (MongoDB).

---

## PHẦN 1: DATA ARCHITECTURE (Kiến trúc dữ liệu)

**Yêu cầu bài toán:**  
Hệ thống E-commerce cần lưu trữ dữ liệu cho **10.000.000 (10 triệu)** đơn hàng.

### 1. Môi trường RDBMS (MySQL hoặc PostgreSQL)
*   Thiết kế Schema chuẩn hóa (Normalized) đạt chuẩn 3NF.
*   Bắt buộc có tối thiểu 3 bảng: `Users`, `Orders` và `OrderItems`.
*   Thiết lập đầy đủ khóa chính (PK), khóa ngoại (FK) và các ràng buộc toàn vẹn.

### 2. Môi trường NoSQL (MongoDB)
*   Thiết kế Schema theo tư duy NoSQL (Document-oriented).

### 3. Data Generation
*   Viết script (SQL/Python/Go/Java/NodeJS...) để tạo **10 triệu** bản ghi giả lập và insert vào cả 2 hệ thống trên.
*   *Lưu ý:* Dữ liệu phải đảm bảo tính ngẫu nhiên về thời gian (trong 2 năm gần nhất), trạng thái đơn hàng và giá trị tiền tệ.

---

## PHẦN 2: READ CHALLENGE

**Bối cảnh:**  
API lấy chi tiết đơn hàng (`GET /orders/{id}`) cần trả về trọn vẹn thông tin bao gồm: *Thông tin User + Thông tin đơn hàng + Danh sách sản phẩm trong đơn*.

**Yêu cầu thực hiện:**
1.  Viết script client thực hiện **5.000 request** lấy chi tiết đơn hàng (ID ngẫu nhiên).
2.  Đo lường:
    *   Thời gian phản hồi trung bình **(Average Response Time)**.
    *   Thời gian chỉ số **P95**.
3.  So sánh hiệu năng giữa query `JOIN` nhiều bảng (SQL) và query document (NoSQL).

---

## PHẦN 3: CONCURRENCY CHALLENGE

**Bối cảnh:**  
Một sản phẩm "Flash Sale" chỉ còn đúng **1 sản phẩm** trong kho (`quantity = 1`).

**Yêu cầu thực hiện:**
1.  Viết script giả lập **50 luồng (Threads)** cùng lúc gửi request đặt mua sản phẩm này.
2.  **Thử thách:**
    *   Đảm bảo tuyệt đối không xảy ra tình trạng "bán âm kho" (Overselling). Tồn kho cuối cùng phải là 0.
    *   Xử lý vấn đề Race Condition trên cả 2 môi trường SQL và NoSQL.
3.  **Báo cáo:** Giải thích cơ chế mà bạn đã sử dụng để giải quyết vấn đề.

---

## PHẦN 4: ANALYTICS & SCALABILITY

**Bối cảnh:**  
Team Business cần báo cáo: *"Tổng doanh thu theo từng tháng trong 2 năm qua"*.

**Yêu cầu thực hiện:**

### 1. Trên SQL
*   Với bảng `Orders` 10 triệu dòng, query thông thường sẽ chậm.
*   Hãy áp dụng kỹ thuật **Table Partitioning** để tối ưu hóa truy vấn này.
*   So sánh `EXPLAIN` plan trước và sau khi Partition.

### 2. Trên NoSQL
*   Sử dụng **Aggregation Framework** để thực hiện báo cáo tương tự.

### 3. So sánh
*   So sánh độ phức tạp khi triển khai và tốc độ thực thi của hai giải pháp.

---

## DELIVERABLES

Nộp lại một file báo cáo (PDF/Markdown) bao gồm:

1.  **Source Code:** 
    *   Script tạo dữ liệu (Data Generator).
    *   Script test performance (Benchmark Tool).
    *   Script xử lý Concurrency.
2.  **Bảng Benchmark:** Số liệu so sánh cụ thể (đơn vị ms) cho các trường hợp Read và Analytics.
3.  **Phân tích kỹ thuật (Technical Analysis):**
    *   Tại sao bạn chọn thiết kế Schema đó cho MongoDB?
    *   Tại sao Partitioning lại giúp tăng tốc query trong trường hợp này?
    *   Cơ chế nào đảm bảo tính toàn vẹn dữ liệu (Data Integrity) trong bài test Concurrency?
    *   Khi nào nên dùng SQL? Khi nào nên dùng NoSQL?

---
*Lưu ý: Ứng viên tự do lựa chọn ngôn ngữ lập trình để viết tool tạo dữ liệu và test.*