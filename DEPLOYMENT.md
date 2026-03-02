# Конфигурационные файлы для различных хостингов

## 1. Procfile для Heroku

```
worker: python bot.py
```

## 2. runtime.txt для Heroku

```
python-3.11.7
```

## 3. fly.toml для Fly.io

```toml
app = "telegram-business-bot"
primary_region = "ams"

[build]
  builder = "paketobuildpacks/builder:base"

[env]
  PORT = "8080"

[[services]]
  internal_port = 8080
  protocol = "tcp"

  [[services.ports]]
    port = 80
    handlers = ["http"]

  [[services.ports]]
    port = 443
    handlers = ["tls", "http"]
```

## 4. railway.json для Railway

```json
{
  "$schema": "https://railway.app/railway.schema.json",
  "build": {
    "builder": "NIXPACKS"
  },
  "deploy": {
    "startCommand": "python bot.py",
    "restartPolicyType": "ON_FAILURE",
    "restartPolicyMaxRetries": 10
  }
}
```

## 5. render.yaml для Render

```yaml
services:
  - type: worker
    name: telegram-business-bot
    env: python
    buildCommand: pip install -r requirements.txt
    startCommand: python bot.py
    envVars:
      - key: PYTHON_VERSION
        value: 3.11.7
```

## 6. Dockerfile (опционально)

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .

CMD ["python", "bot.py"]
```

## 7. docker-compose.yml (опционально)

```yaml
version: '3.8'

services:
  bot:
    build: .
    restart: always
    volumes:
      - ./database:/app/database
      - ./media:/app/media
      - ./exports:/app/exports
    environment:
      - BOT_TOKEN=${BOT_TOKEN}
      - ADMIN_ID=${ADMIN_ID}
```

## 8. .env.example

```bash
BOT_TOKEN=your_bot_token_here
ADMIN_ID=your_telegram_id_here
ADMIN_USERNAME=your_username
```

## Инструкции по деплою

### Heroku
```bash
heroku create your-bot-name
git push heroku main
heroku ps:scale worker=1
heroku logs --tail
```

### Fly.io
```bash
fly launch
fly deploy
fly logs
```

### Railway
```bash
railway init
railway up
railway logs
```

### Render
1. Создайте аккаунт на render.com
2. Подключите GitHub репозиторий
3. Выберите "Web Service"
4. Render автоматически обнаружит Python
5. Укажите команду запуска: `python bot.py`

### Bothost.ru
1. Зарегистрируйтесь на bothost.ru
2. Создайте новый проект
3. Подключите GitHub
4. Укажите главный файл: bot.py
5. Бот запустится автоматически

## Переменные окружения

Для безопасности рекомендуется хранить чувствительные данные в переменных окружения:

```python
import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN', 'default_token')
ADMIN_ID = int(os.getenv('ADMIN_ID', '0'))
```

## Мониторинг

### Логирование в файл
```python
logging.basicConfig(
    filename='bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### Отправка логов админу
```python
async def send_error_to_admin(error_text):
    try:
        await bot.send_message(ADMIN_ID, f"🚨 Ошибка:\n{error_text}")
    except:
        pass
```

## Backup базы данных

### Автоматический бэкап
```python
import shutil
from datetime import datetime

def backup_database():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f"backups/bot_db_{timestamp}.db"
    shutil.copy2('database/bot.db', backup_path)
```

## Проверка работоспособности

```python
async def health_check():
    try:
        me = await bot.get_me()
        return True
    except:
        return False
```
