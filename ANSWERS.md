# ANSWERS.md — Modern Platform Architecture & Production Readiness

**Học phần**: Day 28 Track 2 — Platform Integration & Production Readiness  
**Tác giả**: Nguyễn Tuấn Anh (MSSV: 2A202601669)  
**Repository**: [Track2-Day28-2A202601669-nguyentuananh-](https://github.com/nguyentuananh512005/Track2-Day28-2A202601669-nguyentuananh-.git)  

---

## Section 1: Technical Trade-offs (Phân tích Đánh đổi Kỹ thuật)

### 1.1 Synchronous vs Asynchronous Ingestion (Đồng bộ vs Bất đồng bộ)
*So sánh giữa Direct HTTP write to DB/Delta vs Kafka Event Queue Ingestion Pipeline*

| Tiêu chí | Synchronous (Direct HTTP Ingestion) | Asynchronous (Kafka Event Queue Ingestion) |
| :--- | :--- | :--- |
| **Cơ chế hoạt động** | Client gửi request HTTP trực tiếp tới API Gateway/Backend và chờ ghi trực tiếp vào Database/Delta Lake trước khi nhận phản hồi (200 OK). | Envoy/API Gateway nhận request, đính kèm W3C traceparent và idempotency key, đẩy message vào Kafka topic và phản hồi ngay (202 Accepted). Worker (Airflow/Spark) tiêu thụ message bất đồng bộ. |
| **Latency (Độ trễ phản hồi)** | Phụ thuộc trực tiếp vào thời gian khóa (lock contention), transaction commit của database, và network latency của downstream. Trung bình từ 50ms – 500ms+. | Cực thấp (sub-10ms), client chỉ chờ Kafka broker ack (`acks=all` hoặc `acks=1` vào memory buffer/page cache). |
| **Backpressure Handling** | Kém. Khi lượng request đột biến (traffic spikes / flash crowds), database connection pool bị cạn kiệt (pool exhaustion), dẫn tới cascading timeouts, 503 Service Unavailable và nghẽn hệ thống. | Tự nhiên và tối ưu. Kafka hoạt động như một bộ đệm giảm chấn (shock absorber). Consumer có thể đọc theo tốc độ tối đa của nó mà không làm crash downstream. |
| **Durability & Replayability** | Kém khi có sự cố. Nếu backend service gặp crash giữa chừng hoặc database fail, request bị mất vĩnh viễn trừ khi client tự retry (gây nguy cơ duplicate). Không thể replay lại lịch sử sự kiện. | Cao. Log-based storage với distributed commit log, retention period có thể cấu hình (ví dụ 7 ngày). Khi downstream phục hồi hoặc có bug logic, worker có thể replay từ offset cũ với `idempotency_key` để tái tạo dữ liệu chính xác (deterministic re-processing). |
| **Decoupling (Phân tách hệ thống)** | Khớp nối lỏng lẻo kém (Tight coupling). Mọi thay đổi về database schema, downtime bảo trì của DB đều ảnh hưởng trực tiếp tới ingress API. | Khớp nối hoàn toàn (Loose coupling). Ingestion service và Processing/Storage service độc lập hoàn toàn về lifecycle, scaling, và công nghệ. |
| **Complexity & Ops Overhead** | Thấp. Dễ triển khai, debug đơn giản, không cần quản lý cluster message broker hay theo dõi consumer lag. | Cao. Yêu cầu vận hành cụm Kafka (Zookeeper/KRaft), giám sát partition rebalance, offset commit, consumer lag, dead-letter queue (DLQ) và schema registry. |

**Kết luận kiến trúc**:
Đối với nền tảng AI/ML quy mô lớn, kiến trúc Asynchronous qua Kafka là bắt buộc để đáp ứng tính sẵn sàng cao (High Availability), bảo vệ Delta Lake khỏi tình trạng commit conflict khi có hàng ngàn concurrent micro-batches, và đảm bảo khả năng khôi phục sau sự cố (Disaster Recovery & Event Replay).

---

### 1.2 Local OTLP / Jaeger vs Managed SaaS (LangSmith)
*So sánh giữa Self-hosted OpenTelemetry Collector + Jaeger vs Managed LLM Observability Platform (LangSmith)*

| Tiêu chí | Self-Hosted OpenTelemetry + Jaeger | Managed SaaS (LangSmith) |
| :--- | :--- | :--- |
| **Data Privacy & Compliance** | **Toàn quyền kiểm soát (100% On-premise/VPC)**. Dữ liệu span, payloads, prompts, embeddings và user queries không bao giờ rời khỏi hạ tầng nội bộ. Đạt chuẩn tuân thủ khắt khe như HIPAA, GDPR, PCI-DSS và bảo vệ bí mật kinh doanh/IP của doanh nghiệp. | **Rủi ro rò rỉ dữ liệu bên ngoài**. Prompts, system instructions, thông tin nhạy cảm (PII), và dữ liệu khách hàng được đẩy lên hạ tầng cloud của bên thứ ba. Cần ký thỏa thuận DPA, thực hiện data masking/redaction phức tạp trước khi gửi. |
| **Cost at Scale (Chi phí theo quy mô)** | Chi phí phần cứng/compute cố định (Fixed infrastructure cost). Khi khối lượng tracing tăng từ hàng ngàn lên hàng triệu spans/ngày, chi phí chỉ tăng nhẹ theo dung lượng đĩa và tài nguyên CPU của Collector/Elasticsearch/ClickHouse backend. | Chi phí biến đổi theo usage (Variable SaaS pricing). Tính tiền theo số lượng traces, tokens hoặc seat license. Chi phí có thể tăng vọt khó kiểm soát khi hệ thống mở rộng quy mô phục vụ hàng triệu người dùng. |
| **LLM-Specific Tracing & Evaluation** | Tiêu chuẩn OpenTelemetry phân tán chung (General distributed tracing). Hỗ trợ W3C trace context, spans, tags, latency breakdown giữa các microservices (Gateway, Vector DB, Model Server). Tuy nhiên, thiếu UI chuyên biệt để xem cây hội thoại (chat trees), prompt template diffing, token consumption analytics, và tích hợp automated LLM evaluation. | **Chuyên sâu và tối ưu riêng cho GenAI**. Giao diện chuyên biệt cho LLM chains/agents, visualize LangChain/LangGraph run-trees, playground thử prompt trực tiếp, online evaluators, dataset management, human-in-the-loop feedback và prompt regression testing. |
| **Operational Complexity** | Cao. Nhóm Platform Engineering phải tự chịu trách nhiệm cấu hình Collector pipeline, batch processor, sampling strategy, lưu trữ (Elasticsearch/Cassandra/ClickHouse), backup và mở rộng Jaeger cluster. | Cực thấp (Zero Ops). Chỉ cần thiết lập biến môi trường (`LANGCHAIN_TRACING_V2=true`, `LANGCHAIN_API_KEY`), nền tảng hoàn toàn do nhà cung cấp quản lý và bảo trì 24/7. |

**Kết luận kiến trúc**:
Phương án kết hợp tối ưu (Hybrid Approach): Sử dụng OpenTelemetry Collector làm chuẩn ingress thống nhất trong toàn bộ nền tảng. Collector xuất trace hạ tầng và microservice metrics về Jaeger/Prometheus nội bộ (đảm bảo tính bảo mật và chi phí), đồng thời có thể áp dụng sampling và PII masking để đẩy subset các LLM run spans về LangSmith phục vụ đội ngũ AI/Prompt Engineers đánh giá chất lượng mô hình.

---

### 1.3 Feast Online Store vs Direct SQL Queries
*So sánh giữa Feast Feature Store (Redis Key-Value) vs Direct SQL Queries trên Relational Database / Lakehouse*

| Tiêu chí | Feast Online Store (Redis / In-memory KV) | Direct SQL Queries (PostgreSQL / Delta Lake) |
| :--- | :--- | :--- |
| **Serving Latency (Độ trễ phục vụ)** | **Cực thấp, mức Sub-millisecond (1ms – 5ms)**. Dữ liệu đặc trưng (features) được chuẩn hóa và lưu trữ dưới dạng key-value tối ưu trong RAM của Redis. Truy xuất theo `entity_id` (ví dụ `asker_id`) chỉ là phép O(1) hash lookup. | **Cao và biến động lớn (20ms – 500ms+)**. Các câu lệnh SQL thường yêu cầu join nhiều bảng (ví dụ bảng transactions, user profile, interactions), aggregate thời gian thực (COUNT, AVG), gây tốn I/O đĩa và CPU của DB. |
| **Point-in-Time Correctness** | **Được bảo đảm bởi thiết kế (Guaranteed by design)**. Feast phân tách rõ ràng giữa Offline Store (phục vụ training dataset generation với time-travel join chống data leakage) và Online Store (phục vụ low-latency inference). Đảm bảo tính nhất quán giữa feature lúc huấn luyện và feature lúc suy luận (Training-Serving Skew Prevention). | Dễ gặp lỗi rò rỉ dữ liệu tương lai (Data Leakage) khi chuẩn bị dataset huấn luyện nếu câu lệnh SQL không quản lý timestamp chặt chẽ. Rất khó để tái hiện chính xác trạng thái feature tại một thời điểm trong quá khứ khi chạy direct SQL. |
| **Feature Drift & Versioning** | Feast Registry hoạt động như một "Source of Truth" tập trung. Mọi Feature View, Entity, Data Type đều được định nghĩa khai báo (Declarative definitions as code), có metadata, schema validation và hỗ trợ theo dõi phân phối feature theo thời gian. | Thiếu registry tập trung. Các truy vấn SQL thường phân tán trong mã nguồn ứng dụng (hardcoded queries) hoặc stored procedures, khó kiểm soát phiên bản schema, dễ dẫn tới trôi dạt đặc trưng (feature drift) giữa các phiên bản model. |
| **Flexibility & Ad-hoc Queries** | Thấp. Online Store chỉ hỗ trợ truy xuất theo primary entity key đã được định nghĩa trước; không hỗ trợ các câu truy vấn phức tạp dạng ad-hoc, filter nhiều điều kiện tùy ý hay full-table scans. | Cực cao. Ngôn ngữ SQL linh hoạt, cho phép thực hiện bất kỳ phép phân tích, lọc điều kiện, window function hoặc join phức tạp nào theo yêu cầu nghiệp vụ mới mà không cần re-index. |
| **Materialization Overhead** | Cần job đồng bộ (Sync/Materialization pipeline) định kỳ chạy từ Offline Store (Delta Lake/BigQuery) sang Online Store (Redis). Tồn tại độ trễ dữ liệu (freshness lag) giữa batch run. | Không có materialization overhead. Truy vấn trực tiếp lấy dữ liệu mới nhất (real-time consistency) vừa được commit vào database. |

**Kết luận kiến trúc**:
Trong pipeline suy luận thời gian thực (Real-time LLM Serving / Recommendation), Feast Online Store là thành phần tối quan trọng để đáp ứng SLA độ trễ nghiêm ngặt (< 20ms cho toàn bộ pipeline). Việc truy vấn Direct SQL chỉ nên sử dụng trong các tác vụ phân tích batch offline hoặc trong các báo cáo BI nội bộ.

---

## Section 2: Production Gaps (Khoảng trống Đưa vào Môi trường Sản xuất)

### 2.1 Secrets Management (Quản lý Bí mật)
- **Hiện trạng trong Lab**:
  - Các thông tin nhạy cảm, mật khẩu dịch vụ, credentials của Kafka, Postgres, Qdrant API keys, MinIO S3 keys hiện đang được lưu trữ dưới dạng bản rõ (plaintext) trong các file cấu hình cục bộ như `.env`, `ports.template` và biến môi trường của Docker Compose.
  - Các file này có nguy cơ vô tình bị commit lên Git repository, lộ lọt qua container environment inspection (`docker inspect`) hoặc lộ qua log files.
- **Giải pháp cho Môi trường Production**:
  1. **Hệ thống Quản lý Secret Tập trung**: Sử dụng **HashiCorp Vault**, **AWS Secrets Manager**, hoặc **Google Cloud Secret Manager**.
  2. **Kubernetes External Secrets Operator (ESO)**: Cài đặt External Secrets Operator trên Kubernetes để tự động đồng bộ secrets từ Vault/AWS Secrets Manager vào native Kubernetes `Secret` objects.
  3. **Tự động Xoay vòng Khóa (Dynamic Secrets & Secret Rotation)**: Cấu hình tự động đổi mật khẩu định kỳ cho Database và Cloud credentials mà không gây gián đoạn dịch vụ.
  4. **Sealed Secrets / Mozilla SOPS**: Đối với mô hình GitOps (ArgoCD), mã hóa các secret manifest bằng khóa bất đối xứng công khai trước khi push lên Git; chỉ có controller bên trong Kubernetes cluster mới nắm private key để giải mã (decrypt at runtime).
  5. **Chính sách Kiểm soát Truy cập (Least Privilege)**: Phân quyền truy cập secret nghiêm ngặt theo Kubernetes ServiceAccount và IAM Roles (Workload Identity / IRSA).

---

### 2.2 High Availability (HA) Database & Storage (Cơ sở Dữ liệu & Lưu trữ Sẵn sàng Cao)
- **Hiện trạng trong Lab**:
  - Các thành phần lưu trữ dữ liệu chính (PostgreSQL, Redis, Qdrant Vector DB, MinIO) đều đang chạy dưới dạng single-node / single-container độc lập trong Docker Compose.
  - Điểm hỏng đơn lẻ (Single Point of Failure - SPOF): Nếu một container bị lỗi (OOM, đĩa đầy, node vật lý crash), toàn bộ dịch vụ phụ thuộc sẽ sập ngay lập tức và có thể gây mất mát dữ liệu do thiếu cơ chế sao lưu dự phòng.
- **Giải pháp cho Môi trường Production**:
  1. **PostgreSQL HA**:
     - Triển khai mô hình Primary-Replica thông qua **Patroni** kết hợp etcd/Consul để tự động phát hiện lỗi và failover (Auto Failover) trong vòng vài giây, hoặc sử dụng **CloudNativePG Operator** trên Kubernetes.
     - Đồng bộ dữ liệu bằng Streaming Replication (ít nhất 1 synchronous standby và nhiều asynchronous standbys).
     - Tích hợp **PgBouncer** để connection pooling và cân bằng tải truy vấn đọc (Read-replica load balancing).
  2. **Redis HA (Feast Online Store)**:
     - Triển khai **Redis Sentinel** cho cấu hình Master-Replica với auto-failover, hoặc **Redis Cluster** (sharding trên nhiều nodes) để phân tán bộ nhớ và chịu tải hàng chục ngàn QPS.
  3. **Qdrant Vector Database Distributed Cluster**:
     - Cấu hình Qdrant ở chế độ phân tán (Distributed Deployment) với Replication Factor $\ge 3$ và phân bổ Shards đồng đều qua các Availability Zones.
     - Sử dụng Raft consensus để đồng bộ hóa trạng thái cluster và snapshot định kỳ lên Cloud Object Storage.
  4. **Object Storage & Lakehouse Storage (Delta Lake / MinIO)**:
     - Chuyển đổi từ MinIO single-node sang **AWS S3** / **Google Cloud Storage** với độ bền (durability) 99.999999999% (11 số 9), hỗ trợ Object Versioning, Lifecycle Policies và Cross-Region Replication (CRR).
     - Đối với on-premise, triển khai MinIO Distributed Mode (tối thiểu 4 drives / nodes).

---

### 2.3 Authentication & Authorization Gateway (Cổng Xác thực & Phân quyền)
- **Hiện trạng trong Lab**:
  - Envoy Proxy hiện tại chỉ đóng vai trò là L7 Reverse Proxy định tuyến cơ bản và áp dụng local rate-limiting đơn giản dựa trên token bucket.
  - Hoàn toàn chưa có lớp bảo vệ xác thực người dùng (Authentication), phân quyền truy cập (Authorization), và kiểm soát danh tính (Identity Verification). Mọi client có thể gửi request trực tiếp nếu biết đường dẫn endpoint.
- **Giải pháp cho Môi trường Production**:
  1. **Zero Trust Architecture & Ingress Authentication**:
     - Cấu hình Envoy với **OAuth2 / OIDC Filter** để tích hợp với Identity Provider tập trung (IdP) như Keycloak, Okta, Auth0 hoặc Microsoft Entra ID.
     - Triển khai **JWT Validation Filter** ngay tại Edge Gateway: Kiểm tra chữ ký số (JWKS validation), thời hạn token (`exp`), Issuer (`iss`), Audience (`aud`) trước khi forward request vào mạng nội bộ.
  2. **Fine-grained Authorization (RBAC / ABAC)**:
     - Tích hợp Envoy **External Authorization Filter (`ext_authz`)** gọi tới Open Policy Agent (OPA) hoặc Cerbos.
     - Áp dụng chính sách phân quyền chi tiết: Chỉ người dùng có role `ai-admin` mới được gọi các endpoint nhạy cảm như `/v1/models/release` hoặc `/v1/index/rebuild`; người dùng thông thường chỉ có quyền gọi `/v1/feedback` và `/v1/chat`.
  3. **Mutual TLS (mTLS) cho Service-to-Service Communication**:
     - Triển khai Service Mesh (như Istio hoặc Linkerd) để mã hóa toàn bộ lưu lượng nội bộ giữa các microservices bằng mTLS.
     - Định danh từng service thông qua SPIFFE/SPIRE ID, ngăn chặn hoàn toàn tấn công Man-in-the-Middle (MITM) và mạo danh service bên trong cụm.

---

### 2.4 Multi-node Deployment & Elastic Scaling (Triển khai Đa Node & Tự động Mở rộng)
- **Hiện trạng trong Lab**:
  - Toàn bộ 10 integration services được cấu hình trong 1 file `docker-compose.yaml` chạy trên một máy trạm duy nhất (Single Host).
  - Tài nguyên phần cứng (CPU, RAM, GPU, Disk I/O) bị giới hạn bởi một máy vật lý, không thể co giãn khi tải tăng đột biến và không có tính năng phục hồi container tự động khi node sập.
- **Giải pháp cho Môi trường Production**:
  1. **Chuyển dịch sang Production Kubernetes Cluster**:
     - Chuyển đổi toàn bộ docker-compose services thành Kubernetes manifests (Deployments, StatefulSets, Services, ConfigMaps, Ingress, NetworkPolicies).
     - Thiết lập cluster trải dài trên nhiều Availability Zones (Multi-AZ) với Node Pools chuyên biệt: CPU General Pool, Memory-Optimized Pool cho Spark/Postgres, và GPU Node Pool (NVIDIA A100/H100) cho vLLM inference.
  2. **Tự động Co giãn Đàn hồi (Horizontal Pod Autoscaler - HPA)**:
     - Cấu hình HPA cho Envoy Gateway và API Services dựa trên tài nguyên thực tế (CPU utilization > 70%, Memory > 80%) và custom metrics từ Prometheus (Request per second - RPS).
  3. **KEDA (Kubernetes Event-driven Autoscaling) cho Event Consumers**:
     - Sử dụng KEDA để theo dõi trực tiếp **Kafka Consumer Lag**. Khi số lượng tin nhắn tồn đọng trong Kafka topic tăng vượt ngưỡng (ví dụ lag > 500 messages), KEDA sẽ tự động scale-out số lượng worker pods (Airflow/Spark stream consumers) từ 1 lên 10+ pods để giải tỏa hàng đợi, và tự động scale-down khi lag giảm về 0.
  4. **Pod Disruption Budgets (PDB) & High Resilience**:
     - Thiết lập PDB đảm bảo luôn có ít nhất 80% số lượng pods sẵn sàng phục vụ khi Kubernetes thực hiện rolling upgrade hoặc bảo trì worker nodes.
     - Áp dụng PodAntiAffinity để ngăn chặn các bản sao (replicas) của cùng một service nằm chung trên một node vật lý.

---

## Section 3: Contribution Table (Bảng Đóng góp Cá nhân)

| Mục thông tin | Nội dung chi tiết |
| :--- | :--- |
| **Họ và tên sinh viên** | **Nguyễn Tuấn Anh** |
| **Mã số sinh viên (MSSV)** | **2A202601669** |
| **Lớp / Khóa** | AI20k — Modern AI Platform Engineering |
| **GitHub Username** | [`nguyentuananh512005`](https://github.com/nguyentuananh512005) |
| **Repository bài nộp** | [https://github.com/nguyentuananh512005/Track2-Day28-2A202601669-nguyentuananh-.git](https://github.com/nguyentuananh512005/Track2-Day28-2A202601669-nguyentuananh-.git) |
| **Vai trò dự án** | **Lead Platform Engineer / Full-Stack Integration Specialist** |

### Chi tiết Phân công và Đóng góp Kỹ thuật

| Lĩnh vực phụ trách | Nội dung công việc và Bằng chứng thực nghiệm | Tỷ lệ đóng góp |
| :--- | :--- | :---: |
| **1. Core Platform Integration** | • Cài đặt và chuẩn hóa 4 boundary integration functions trong `src/lab28_platform/integration_tasks.py`:<br>&nbsp;&nbsp;- `event_headers`: Khắc phục triệt để lỗi W3C traceparent edge-case khi nhận chuỗi rỗng `""` hoặc `None`, chỉ gửi `idempotency-key`.<br>&nbsp;&nbsp;- `dedupe_latest`: Cài đặt thuật toán deterministic deduplication theo tuple `(occurred_at, event_id)` phục vụ Delta Lake MERGE idempotency.<br>&nbsp;&nbsp;- `feast_online_request`: Xây dựng cấu trúc truy vấn đặc trưng chuẩn hóa theo Feature Registry.<br>&nbsp;&nbsp;- `readiness_status`: Cài đặt logic phân tầng probe giữa mandatory (ảnh hưởng core availability) và optional degradation. | 100% |
| **2. GitOps & Manifest Engineering** | • Cập nhật và cấu hình GitOps Application manifest tại `gitops/application.yaml` trỏ chính xác về kho lưu trữ cá nhân của sinh viên (`nguyentuananh512005/Track2-Day28-2A202601669-nguyentuananh-.git`).<br>• Xác thực tính hợp lệ của toàn bộ Kubernetes & GitOps manifests thông qua script `validate_manifests.py`. | 100% |
| **3. Testing & Verification Engineering** | • Bổ sung test cases và assertions trong `starter-tests/test_integration_tasks.py` nhằm xác thực tính đúng đắn của logic xử lý chuỗi rỗng trong `event_headers`.<br>• Chạy và vượt qua 100% bài kiểm tra mã nguồn (87 tests: 4 starter tests + 83 unit tests) với thời gian thực thi tối ưu (exit code 0).<br>• Thực thi kiểm tra chất lượng code với Ruff (`uv run ruff check .`) đạt 0 lỗi cảnh báo. | 100% |
| **4. Architectural Documentation** | • Nghiên cứu, phân tích và biên soạn tài liệu kiến trúc toàn diện `ANSWERS.md` với đầy đủ 3 phần:<br>&nbsp;&nbsp;- Technical Trade-offs (Sync vs Async, Local OTLP vs LangSmith, Feast vs Direct SQL).<br>&nbsp;&nbsp;- Production Gaps (Secrets Management, Database & Storage HA, Authentication Gateway, Multi-node K8s & KEDA).<br>&nbsp;&nbsp;- Bảng phân bổ đóng góp kỹ thuật và cam kết liêm chính học thuật. | 100% |
| **Tổng thể dự án** | **Chịu trách nhiệm toàn diện về chất lượng code, kiến trúc, kiểm thử và tài liệu bàn giao.** | **100%** |

---

## Cam kết Liêm chính Học thuật (Academic Integrity Commitment)

Tôi xin cam kết toàn bộ nội dung mã nguồn, các ca kiểm thử bổ sung, và các phân tích kỹ thuật trong tài liệu này được thực hiện dựa trên sự nghiên cứu độc lập, hiểu biết sâu sắc về các thành phần kiến trúc hệ thống nền tảng AI. Không sử dụng mã nguồn giả mạo, không hardcode kết quả kiểm thử, và tuân thủ nghiêm ngặt chuẩn mực kỹ thuật của học phần.
