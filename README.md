# Highload Trading Aggregator (.NET 9)

Высокопроизводительный отказоустойчивый сервер агрегации и обработки ценовых данных (котировок) с криптовалютных бирж в реальном времени.

Система спроектирована для работы под постоянной нагрузкой 500–1000+ тиков/сек с защитой от сетевых аномалий, повреждённых данных (Poison Pills), кратковременных падений базы данных и утечек памяти.

---

## Быстрый запуск

### 1. Запуск в Docker (Рекомендуемый)

Требования: Установленные Docker и Docker Compose.

1. Склонируйте репозиторий и перейдите в корень проекта.
2. Соберите и поднимите весь комплекс (PostgreSQL, Mock-сервер бирж, Агрегатор) в фоновом режиме:
   docker compose up --build -d
3. Проверьте статус контейнеров:
   docker compose ps

---

## Доступные эндопоинты

После запуска агрегатор доступен по адресу http://localhost:5000:

- GET /health/live — Liveness Probe (Проверяет, жива ли Kestrel-сессия приложения).
- GET /health/ready — Readiness Probe (Проверяет подключение и готовность кластера PostgreSQL).
- GET /metrics — Prometheus Metrics (Телеметрия: вычитанные/записанные тики, загрузка канала, статистика GC и памяти).

---

## Запуск модульных и нагрузочных тестов

В проект включен набор xUnit тестов, проверяющих гипотезы хаос-инжиниринга и потокобезопасность под конкуренцией:

dotnet test Aggregator.Tests/Aggregator.Tests.csproj

- FailureIsolationTests.cs: Проверяет, что при полном краше сокета одного адаптера (Binance) соседний адаптер (Coinbase) продолжает непрерывно поставлять данные.
- LockFreeDeduplicatorTests.cs: Моделирует конкурентную дедупликацию из 10 параллельных потоков (10 x 5000 итераций) и детерминированно сдвигает время с помощью .NET 8 FakeTimeProvider.

---

## Ключевые инженерные и архитектурные решения

### 1. Zero-Allocation Hot Path (Сетевой слой и JSON)
- System.IO.Pipelines + PipeReader: Чтение байтов из WebSocket происходит без выделения byte[] в LOH/GC.
- Streaming Utf8JsonReader: Парсинг JSON осуществляется непосредственно поверх ReadOnlySequence<byte> с флагом isFinalBlock: false.
- String Pooling & Fast Hash: Для строковых тикеров применяется StringPool (CommunityToolkit.HighPerformance) и хеширование FNV-1a над ReadOnlySpan<byte>. Это свело на нет аллокации строк при повторяющихся тикерах.
- Pre-allocated Metrics: Метрики Prometheus используют предварительно созданный массив Counter.Child[], убирая выделение строк-лейблов при вызове PublishAsync.

### 2. Потокобезопасная дедупликация и защита от OOM
- Ключ дедупликации: Композитный readonly record struct TickKey(ExchangeSource, TickerId, TimestampMs, Price, Volume).
- O(1) Lock-Free eviction: Дедупликация построена на ConcurrentDictionary и ConcurrentQueue со скользящим окном в 5 секунд.
- OOM Memory Guard: Чтобы избежать переполнения RAM при спам-атаках уникальными тиками, внедрен lock-free предел MaxCapacity = 500_000. При его превышении дедупликатор переходит в быстрый bypass-режим (Volatile.Read / Interlocked), сохраняя работоспособность сервиса.
- Double-Checked Locking: Регистрация новых тикеров в TickerMapper защищена через новый примитив .NET 9 System.Threading.Lock.

### 3. Защита от Poison Pills и сетевых обрывов (Polly Resiliency)
- Fail-Fast на поврежденных фреймах: Если имитатор биржи высылает невалидный/оборванный JSON, Utf8JsonReader выбрасывает JsonReaderException. Адаптер ловит его, выбрасывает InvalidDataException, жестко разрывает сокет и сбрасывает pipe-буфер, предотвращая десинхронизацию TCP-потока.
- Polly Pipeline ws-retry: Все сетевые ошибки обёрнуты в ретрай-пайплайн с экспоненциальным backoff, Jitter'ом и жестким верхним ограничением паузы MaxDelay = 30s.
- Half-Open Detection: Для обнаружения "тихих" обрывов сети задействован Idle Timeout = 15s.

### 4. PostgreSQL Bulk Upsert & Защита системного каталога
- COPY BINARY: Запись выполняется батчами по 1000 элементов (или каждые 500 мс). Вместо поштучных INSERT используется бинарный импорт Npgsql.BeginBinaryImportAsync.
- Предотвращение Catalog Bloat: Вместо ON COMMIT DROP используется CREATE TEMP TABLE IF NOT EXISTS temp_ticks ... ON COMMIT DELETE ROWS;. Это переиспользует структуру временной таблицы в рамках сессии и спасает системный каталог PostgreSQL (pg_class/pg_attribute) от катастрофического раздувания при 1000+ батчей в минуту.
- Идемпотентность: Финальное слияние выполняется через INSERT INTO Ticks SELECT FROM temp_ticks ON CONFLICT DO NOTHING.

### 5. Гарантия сохранности данных (Zero Data Loss) & Backpressure
- Стратегия при отказе БД: При ошибке записи в PostgreSQL буфер List<Tick> НЕ очищается. Воркер BatchProcessorWorker делает паузу Task.Delay(5s) и повторяет попытку сохранения тех же данных.
- Распространение Backpressure: Пока воркер заблокирован ретраем в БД, вычитка из TickChannelBus приостанавливается. Шина (Capacity = 100_000, BoundedChannelFullMode.Wait) забивается до предела, после чего асинхронный PublishAsync приостанавливает вычитку из сокетов. Данные не теряются.

### 6. Graceful Shutdown (Штатное завершение)
- При сигнале остановки контейнера HostOptions.ShutdownTimeout выделяет 15 секунд.
- TickChannelBus переводится в состояние Complete(), запрещая новый прием.
- BatchProcessorWorker выгребает оставшиеся тики из памяти (drain loop) и дописывает финальный батч в PostgreSQL через COPY BINARY.

---

## Мониторинг и Метрики (Prometheus)

Агрегатор экспортирует следующие ключевые бизнес-метрики:

- aggregator_ticks_ingested_total{exchange="..."} — общее число тиков, вычитанных из сокетов каждой биржи.
- aggregator_ticks_written_total — общее количество тиков, успешно записанных в PostgreSQL.
- aggregator_channel_fullness_ratio — коэффициент заполненности in-memory канала (от 0.0 до 1.0), индикатор backpressure.

---

## Известные ограничения (Trade-offs)

1. Гарантия доставки: На сетевом слое используется модель At-Least-Once Delivery. Повторно доставленные тики отсеиваются дедупликатором в памяти или падают в ON CONFLICT DO NOTHING на уровне PostgreSQL.
2. Поведение при критически долгом аутинге БД: При падении базы данных более чем на несколько минут in-memory буфер TickChannelBus заполнится до 100k элементов (потребление RAM ~40 МБ), после чего сокеты перестанут вычитывать данные из TCP-окна. Для Production-окружения следующего уровня рекомендуется внедрить сброс аварийных батчей на диск (Local WAL / RocksDB / Disk Spooling)