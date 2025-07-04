# YouTube в S3 Загрузчик
Скачивание видео с YouTube и загрузка в S3 с помощью команд uv.

## Настройка
```bash
uv sync
```

Создайте файл `.env`:
```env
AWS_ACCESS_KEY_ID=ваш_access_key_id
AWS_SECRET_ACCESS_KEY=ваш_secret_access_key
AWS_STORAGE_BUCKET_NAME=имя_вашего_бакета
AWS_S3_REGION_NAME=us-east-1
AWS_S3_ENDPOINT_URL=https://ваш-endpoint.com
```

## Команды

**Проверить конфигурацию:**
```bash
uv run check-config
```

**Скачать только видео:**
```bash
uv run download "https://www.youtube.com/watch?v=VIDEO_ID"
```

**Загрузить локальный файл:**
```bash
uv run upload video.mp4 --folder "мои_видео"
```

**Скачать и загрузить (основная команда):**
```bash
uv run youtube-s3 "https://www.youtube.com/watch?v=VIDEO_ID"
uv run youtube-s3 "https://www.youtube.com/watch?v=VIDEO_ID" --folder "мои_видео"
```

**Показать содержимое бакета:**
```bash
uv run list-bucket
uv run list-bucket --folder "youtube"
```
