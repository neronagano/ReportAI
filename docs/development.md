# Разработка

## Локальное окружение

Добавлены `pyproject.toml`, `uv.lock` и `.python-version`. Для разработки
используются Python 3.12 и uv `0.10.9`; инфраструктура запускается через Docker.
Версия uv проверяется настройкой `tool.uv.required-version` в `pyproject.toml`.
Установка нужной версии описана в [документации uv](https://docs.astral.sh/uv/getting-started/installation/#installing-specific-versions).

```bash
uv sync --locked
docker compose up -d postgres redis kafka storage
uv run --locked alembic upgrade head
uv run --locked python -m shiftledger.cli bootstrap
uv run --locked python -m shiftledger.cli seed
uv run --locked uvicorn shiftledger.main:create_app --factory --reload --port 18000
```

`.env.example` содержит адреса для процессов на хосте. Compose переопределяет
их на внутренние DNS-имена контейнеров. При изменении портов и паролей в `.env`
адреса `DATABASE_URL`, `REDIS_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `S3_ENDPOINT_URL`
для запуска на хосте обновляются вместе с ними. Спецсимволы в URI-паролях требуют URL-encoding.

Если API и воркеры уже запущены через Compose, для перехода на локальный API
останавливается сервис `api`, чтобы освободить его порт:

```bash
docker compose stop api
```

Воркеры можно оставить в контейнерах либо запускать из исходников:

```bash
uv run --locked python -m shiftledger.workers.outbox
uv run --locked python -m shiftledger.workers.reconciliation
```

Два воркера одного типа намеренно работают совместно. При локальном запуске
двух экземпляров на одном хосте порт метрик одинаков; для такого масштабирования
используются контейнеры с отдельными сетевыми пространствами.

## Управление зависимостями

Зависимости приложения находятся в `project.dependencies`, инструменты разработки —
в `dependency-groups.dev`. `uv sync --locked` устанавливает обе группы; Docker runtime
собирается с `uv sync --locked --no-dev --no-editable`.

```bash
uv add package-name
uv add --dev package-name
uv remove package-name
uv lock --upgrade-package package-name
uv sync --locked
uv lock --check
```

Команды показаны как отдельные операции; `package-name` заменяется именем пакета.
`uv add` и `uv remove` обновляют описание проекта, lock-файл и локальное окружение.
Изменения зависимостей сохраняются вместе с `uv.lock` и проверяются перед коммитом.
Обычный запуск через `uv run --locked` использует зафиксированные версии.

CI получает версию uv из `pyproject.toml`, проверяет lock-файл и синхронизирует окружение.
Для обновления самого uv одновременно меняются `tool.uv.required-version` и
версия/digest uv в `Dockerfile`. Затем выполняются локальные проверки и Docker-сборка.

## Проверки качества

```bash
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked mypy src
uv run --locked pytest tests/unit
docker compose run --rm --build test
```

Для запуска integration-тестов из PowerShell:

```powershell
$env:TEST_DATABASE_URL='postgresql+asyncpg://shiftledger:shiftledger-local-postgres@127.0.0.1:15432/shiftledger_test'
$env:TEST_REDIS_URL='redis://:shiftledger-local-redis@127.0.0.1:16379/15'
uv run --locked python scripts/test.py
```

Для Bash:

```bash
TEST_DATABASE_URL='postgresql+asyncpg://shiftledger:shiftledger-local-postgres@127.0.0.1:15432/shiftledger_test' \
TEST_REDIS_URL='redis://:shiftledger-local-redis@127.0.0.1:16379/15' \
uv run --locked python scripts/test.py
```

Runner допускает только имя `shiftledger_test`. Перед каждым integration-тестом
очищаются таблицы этой БД. Запуск plain `pytest` без `TEST_DATABASE_URL` и
`TEST_REDIS_URL` явно пропускает integration-тесты; такой запуск не заменяет
проверку через Docker/test runner.

Для smoke-проверок после запуска всего стека:

```bash
uv run --locked python scripts/smoke.py
uv run --locked python scripts/check_messaging.py
```

Дополнительная проверка восстановления выполняется на хосте с доступным Docker CLI:

```bash
uv run --locked python scripts/check_recovery.py
```

Скрипт временно останавливает Kafka только проекта `shiftledger`, отправляет
отчёт, возвращает брокер в `finally` и ожидает завершения сверки.

Каждый smoke-запуск использует отдельный `location_code`, поэтому данные
предыдущей проверки не мешают следующей. Тестовое изображение служит для проверки
передачи файла и не имитирует работу OCR.

## Изменение схемы

После изменения моделей создаётся **новая** миграция:

```bash
uv run --locked alembic revision --autogenerate -m "add receipt extractions"
uv run --locked alembic upgrade head
uv run --locked alembic check
```

Сгенерированные операции проверяются вручную: ограничения, tenant-связи,
индексы, default-значения, совместимость с уже записанными данными.
Триггеры и произвольные SQL-объекты добавляются вручную; autogenerate не
заменяет эту проверку. Уже применённые миграции не переписываются.
`Base.metadata.create_all()` в startup не используется.

Изменение опубликованных форм выполняется новой версией через API.
Для миграции данных, защищённых триггером неизменяемости, нужен отдельный
контролируемый план с историей изменений.

## Добавление функции

1. Описывается конкретный бизнес-сценарий и его ограничения.
2. Правило добавляется в соответствующий `domain.py` и проверяется примерами.
3. Операция хранения добавляется в `service.py`, изменение БД — отдельной миграцией.
4. В `router.py` проверяются организация, роль и доступ к конкретному объекту.
5. Для фонового действия сохраняется событие через Outbox в той же транзакции.
6. Consumer обрабатывает повторную доставку безопасно и подтверждает offset после commit.
7. Обновляются README/roadmap и тесты затронутого сценария.

Прямые вызовы Kafka producer из HTTP-обработчика и фоновые задачи `BackgroundTasks`
для критичной обработки отчёта не используются: завершение процесса не должно
терять подтверждённую пользователю операцию.

## Рекомендуемая следующая задача

Первый отдельный этап — извлечённые данные чеков:

- Добавляется таблица `receipt_extractions` с tenant-связью на вложение,
  статусом, результатом, провайдером, моделью и версией схемы.
- После загрузки вложения в одной SQL-транзакции создаётся событие для OCR-воркера.
- Подключается адаптер `ReceiptExtractor` с таймаутом и типизированным ответом.
- Сохраняются неопределённости; исходный файл и сумма сотрудника остаются доступными.
- Добавляется детерминированное сравнение суммы расхода и распознанного итога.
- Проверяются повторная обработка, чужой документ, плохое изображение и ошибка провайдера.

После этого появляется предметная основа для ИИ-объяснений и агента:
проверяемые факты, документы, текст и ссылки на источники.
