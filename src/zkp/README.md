# Доказ для нарахування репутації

AgentService обчислює `0,30 × S(T) + 0,40 × (1 − Tx(T)) + 0,30 × Q(T)`. Кожна ймовірність квантується до чотирьох десяткових знаків, а композитний бал обчислюється в цілих одиницях `1/1 000 000`. Для доказу потрібні одночасно бал `> 0,7` і токсичність `≤ 0,5`. Друге правило зберігає наявний фільтр модерації. ClickHouse отримує фактичний бал навіть для відхилених постів.

Posts зберігає випадкову 32-байтову сіль разом із постом. Commitment — це `Poseidon(Poseidon(author, postId, textDigest), saltHigh, saltLow)`, де `author` і `textDigest` — перші 31 байт SHA-256 від UTF-8 ідентифікатора автора та тексту аналізу. Сіль надходить агенту через окремий HTTPS-ендпоінт. Circom перевіряє відкриття збереженого commitment, діапазони трьох метрик, формулу і обидві умови допуску. Публічні сигнали Groth16 містять commitment поста та окремий Poseidon commitment композитного бала. Валідатор отримує доказ і ці два сигнали, а ClickHouse отримує оцінки окремим каналом аналітики. NLP-оцінки для свідка надає довірений AgentService поза схемою.

## Локальний запуск

1. Запустіть Docker Desktop. У PowerShell із каталогу `src` активуйте Python-середовище з потрібними пакетами. Якщо середовище нове, встановіть залежності командою `python -m pip install aio-pika clickhouse-driver fastapi httpx langgraph torch transformers uvicorn`. Додайте Node.js і npm до `PATH`. Агент шукає локальні моделі `sentiment`, `toxicity` і `quality` у каталозі `models`. Для іншого каталогу задайте `NLP_MODELS_PATH`.
2. Заповніть `trust-system.json` у каталозі `src`. Якщо файлу ще немає, скопіюйте `trust-system.example.json`. Задайте `POST_WITNESS_TOKEN` спільно для Posts і AgentService, а `REPUTATION_SERVICE_TOKEN` спільно для Posts, Users і ConsensusService. Під час `launch.bat` непорожні змінні середовища мають пріоритет над JSON. `null` залишає параметр зі значенням, яке використовує сервіс або launcher за замовчуванням. Git ігнорує локальний `trust-system.json`.

   Для першого налаштування за відсутності токенів можна створити два окремі значення та зберегти їх у JSON і .NET User Secrets для режиму Development.

   ```powershell
   $trustConfig = Get-Content .\trust-system.json -Raw | ConvertFrom-Json
   $trustConfig.POST_WITNESS_TOKEN = python -c "import secrets; print(secrets.token_hex(32))"
   $trustConfig.REPUTATION_SERVICE_TOKEN = python -c "import secrets; print(secrets.token_hex(32))"
   $trustConfig | ConvertTo-Json -Depth 8 | Set-Content .\trust-system.json -Encoding utf8
   dotnet user-secrets set POST_WITNESS_TOKEN $trustConfig.POST_WITNESS_TOKEN --project Posts/Posts.WebApi
   dotnet user-secrets set REPUTATION_SERVICE_TOKEN $trustConfig.REPUTATION_SERVICE_TOKEN --project Posts/Posts.WebApi
   dotnet user-secrets set REPUTATION_SERVICE_TOKEN $trustConfig.REPUTATION_SERVICE_TOKEN --project Users/Users.WebApi
   ```

3. Застосуйте SQL Server міграції та запустіть .NET API окремо, наприклад із Visual Studio. Posts застосовує свої міграції під час старту. Для Users виконайте `dotnet ef database update --project Users/Users.Infrastructure.Data --startup-project Users/Users.WebApi --context UserDbContext` із налаштованим підключенням до SQL Server. Історичні commitment зберігаються. Події без приватного відкриття обробляються лише для аналітики. Довіртеся локальному HTTPS-сертифікату ASP.NET через `dotnet dev-certs https --trust` або задайте PEM CA для Python через `REPUTATION_CA_FILE`. Типове значення `POST_SERVICE_URL` — `https://localhost:7028`, а `REPUTATION_URL` — `https://localhost:7075/user/updatereputation`. Задайте ці змінні, якщо використовуєте інші адреси.
4. Виконайте `.\launch.bat`. Launcher піднімає RabbitMQ і ClickHouse через Docker Compose, а потім відкриває вікна з логами AnalyticsService, ConsensusService та AgentService. Дочекайтеся `Listening for post events` перед створенням поста. Для `CreatePost` потрібен підписаний токен IdentityServer з аудиторією `Posts.WebApi` і ідентифікатором автора. Для іншої адреси IdentityServer задайте `JwtBearer__Authority` у процесі Posts.

За першого запуску launcher встановлює відсутні npm-залежності через `npm ci` та збирає локальні ZKP-артефакти, якщо всі три файли відсутні. Наявний комплект повторно використовується. Щоб використати підготовлений комплект, задайте `ZKP_ARTIFACT_DIR` із `reputation_js/reputation.wasm`, `reputation.zkey` і `verification_key.json`. Для робочого середовища використовуйте артефакти перевіреної багатосторонньої церемонії з тією самою схемою.

Команда `.\launch.bat --check` перевіряє підготовлене середовище без запуску сервісів. Типові параметри `RABBITMQ_URL` і `CLICKHOUSE_*` відповідають локальним контейнерам у Docker Compose. Якщо RabbitMQ і ClickHouse вже запущені окремо, задайте `"TRUST_SKIP_DOCKER": true` у JSON і вкажіть параметри підключення до цих сервісів. Для Grafana виконайте `docker compose up -d grafana`.

Для дашбордів використовуйте `post_analytics_with_consensus`. Представлення поєднує історичний `consensus_result` і нові результати навіть за різного порядку запису подій. `scoring_version = 'composite-v1'` відокремлює вимірювання за новою формулою. `post_consensus` містить кількість голосів і кворум. `NO_QUORUM` означає, що валідний доказ повернеться в чергу для повторної спроби.

## Валідатори

Якщо `CONSENSUS_VALIDATOR_CONFIG` має значення `null`, `launch.bat` запускає 10 окремих процесів `ValidatorService` на портах 8031–8040. `start_consensus_cluster.py` генерує ключ для кожного процесу й передає список адрес і ключів координатору. Кожен процес незалежно перевіряє Groth16. Координатор перевіряє доказ і чекає щонайменше сім позитивних голосів. Кожен голос автентифіковано окремим ключем і прив’язано до доказу та випадкового ідентифікатора спроби.

Для підключення до вже запущених валідаторів задайте `CONSENSUS_VALIDATOR_CONFIG` у `trust-system.json` як масив об’єктів `{"url": "https://validator.example/verify", "key": "<64 hex-символи>"}`. Потрібні щонайменше чотири різні URL і різні ключі. Кожен вузол `ValidatorService` отримує свій ключ через `REPUTATION_VALIDATOR_KEY`, а координатор отримує список із JSON. У цьому режимі launcher запускає координатор, який звертається до вказаних вузлів. `CONSENSUS_CA_FILE` задає довірений PEM CA, `CONSENSUS_VOTE_TIMEOUT` задає час очікування в секундах. Типове значення — 15. Для прямого запуску `python ConsensusService.py` список можна передати через однойменну змінну середовища.

## Відтворення перевірок

Після запуску `launch.bat` дочекайтеся `Listening for post events`. Команда `Test_Scripts\run_normal_traffic.bat --count 1` надсилає синтетичний допис безпосередньо в RabbitMQ. Агент обчислює бал, а AnalyticsService зберігає його в ClickHouse із `proof_generated = false`. Без `--count` генератор надсилає 50 дописів з інтервалом 6 секунд.

Для наскрізної перевірки запустіть Posts і Users з актуальними міграціями. Задайте в PowerShell `POSTS_ACCESS_TOKEN` як змінну середовища з підписаним токеном тестового користувача для аудиторії `Posts.WebApi`. Команда `Test_Scripts\run_normal_traffic.bat --api` створює один справжній допис через `POST /post/CreatePost`. Запит запускає `PostCreatedHandler`, а доказ і кворум залежать від бала допису. `--count` дозволяє задати іншу кількість дописів.

Після `npm run setup:dev` виконайте з `src`:

```powershell
python -m unittest Test_Scripts.test_reputation -v
python -m unittest Test_Scripts.test_validator_cluster -v
```

Перший тест перебирає всі 176 розміщень від нуля до трьох несправних валідаторів. Другий запускає десять процесів і надсилає справжній доказ через HTTP. Один недоступний валідатор і два підписані голоси проти залишають 7/10 голосів за. Вимкнення ще одного валідатора залишає 6/10, і доказ втрачає кворум. Межа 30% стосується валідаторів у мережевому протоколі з довіреним координатором і доступним брокером. Стійкість самого координатора чи брокера потребує окремого експерименту.
