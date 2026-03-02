# Telegram Business API Documentation

## Как работает Business Bot API (2026)

### Обзор

Telegram Business Bot API позволяет ботам подключаться к бизнес-аккаунтам пользователей и обрабатывать сообщения от их клиентов. Это отличается от обычных ботов, которые работают только в собственных чатах.

### Основные концепции

#### Business Connection
Когда пользователь подключает бота к своему бизнес-аккаунту через настройки Telegram Business, создается `BusinessConnection`. Это соединение содержит:
- Уникальный `connection_id`
- Информацию о пользователе (`user`)
- Информацию о бизнес-аккаунте (`user_chat_id`)
- Разрешения (например, `can_reply`)
- Настройки управления сообщениями

#### Business Messages
После подключения бот получает следующие типы обновлений:

1. **business_message** - новые сообщения от клиентов бизнес-аккаунта
2. **edited_business_message** - отредактированные сообщения
3. **deleted_business_messages** - уведомления об удаленных сообщениях

### Типы обновлений

#### 1. Business Connection Update

Получается когда:
- Пользователь подключает бота
- Пользователь изменяет настройки подключения
- Пользователь отключает бота

```python
@router.business_connection()
async def on_business_connection(business_connection: BusinessConnection):
    connection_id = business_connection.id
    user_id = business_connection.user.id
    can_reply = business_connection.can_reply
    is_enabled = business_connection.is_enabled
```

#### 2. Business Message Update

Получается при новом сообщении в бизнес-чате:

```python
@router.business_message()
async def on_business_message(message: Message):
    connection_id = message.business_connection_id
    chat_id = message.chat.id
    message_id = message.message_id
    text = message.text
    from_user = message.from_user
```

#### 3. Edited Business Message Update

Получается при редактировании сообщения:

```python
@router.edited_business_message()
async def on_edited_message(message: Message):
    connection_id = message.business_connection_id
    message_id = message.message_id
    new_text = message.text
    # Сравните с сохраненной версией
```

#### 4. Deleted Business Messages Update

Получается при удалении сообщений:

```python
@router.deleted_business_messages()
async def on_deleted_messages(deleted: BusinessMessagesDeleted):
    connection_id = deleted.business_connection_id
    chat = deleted.chat
    message_ids = deleted.message_ids  # Список удаленных ID
```

### Важные ограничения

#### Что бот МОЖЕТ:
- Получать сообщения из подключенных бизнес-чатов
- Отслеживать изменения в этих сообщениях
- Получать уведомления об удалениях
- Отвечать от имени бизнес-аккаунта (если разрешено)
- Скачивать медиафайлы из полученных сообщений

#### Что бот НЕ МОЖЕТ:
- Видеть личные чаты пользователя
- Получать доступ к сообщениям до подключения
- Восстанавливать содержимое удаленных сообщений (только ID)
- Читать сообщения с таймером самоуничтожения после истечения
- Получать сообщения из групп/каналов пользователя

### Структура BusinessMessagesDeleted

```python
class BusinessMessagesDeleted:
    business_connection_id: str  # ID подключения
    chat: Chat                    # Информация о чате
    message_ids: List[int]       # Список ID удаленных сообщений
```

**ВАЖНО**: Объект содержит только ID удаленных сообщений, но НЕ их содержимое. Поэтому важно сохранять сообщения при получении через `business_message`.

### Работа с медиа

Медиафайлы сохраняются следующим образом:

```python
# Получение file_id
if message.photo:
    file_id = message.photo[-1].file_id  # Самое большое фото
elif message.video:
    file_id = message.video.file_id
elif message.document:
    file_id = message.document.file_id

# Скачивание файла
file = await bot.get_file(file_id)
await bot.download_file(file.file_path, destination)
```

### Отправка сообщений от имени бизнеса

Если `can_reply = True`:

```python
await bot.send_message(
    chat_id=chat_id,
    text="Ответ от бизнеса",
    business_connection_id=connection_id
)
```

### Best Practices

#### 1. Сохранение сообщений

Всегда сохраняйте полученные сообщения в базу данных:
- Текст
- Медиафайлы
- Метаданные (отправитель, время)

Это позволит уведомлять об удалениях с полной информацией.

#### 2. Обработка ошибок

```python
try:
    # Обработка сообщения
except Exception as e:
    logger.error(f"Ошибка: {e}")
    # Не прерывайте работу бота
```

#### 3. Асинхронность

Все операции должны быть асинхронными:
```python
async def download_media():
    async with aiofiles.open(path, 'wb') as f:
        await f.write(content)
```

#### 4. Базы данных

Используйте индексы для быстрого поиска:
```sql
CREATE INDEX idx_message_lookup 
ON saved_messages(user_id, chat_id, message_id);
```

#### 5. Хранение файлов

Организуйте файловую структуру:
```
media/
  {user_id}/
    photo_{timestamp}_{hash}.jpg
    video_{timestamp}_{hash}.mp4
```

### Пример полного workflow

```python
# 1. Пользователь подключает бота
@router.business_connection()
async def on_connection(connection):
    save_connection_to_db(connection)
    notify_user_about_connection()

# 2. Приходит новое сообщение
@router.business_message()
async def on_message(message):
    # Сохраняем в БД со всеми данными
    save_message_to_db(message)
    
    # Скачиваем медиа если есть
    if message.photo:
        await download_and_save_photo(message.photo)

# 3. Сообщение редактируется
@router.edited_business_message()
async def on_edit(message):
    original = get_message_from_db(message.message_id)
    update_message_in_db(message)
    notify_user_about_edit(original, message)

# 4. Сообщение удаляется
@router.deleted_business_messages()
async def on_delete(deleted):
    for msg_id in deleted.message_ids:
        original = get_message_from_db(msg_id)
        mark_as_deleted_in_db(msg_id)
        notify_user_about_deletion(original)
```

### Типичные проблемы и решения

#### Проблема: "NoneType object has no attribute 'get'"

**Причина**: Попытка доступа к атрибутам None объекта.

**Решение**:
```python
if deleted and deleted.business_connection_id:
    connection = db.get_connection(deleted.business_connection_id)
    if connection:
        # Обработка
```

#### Проблема: "Unknown connection_id"

**Причина**: Подключение не сохранено в БД.

**Решение**:
- Всегда сохраняйте подключение при получении `business_connection`
- Проверяйте наличие подключения перед обработкой

#### Проблема: Медиафайлы не скачиваются

**Причина**: Неправильный путь или отсутствие прав.

**Решение**:
```python
path = Path(media_dir)
path.mkdir(parents=True, exist_ok=True)
```

### Мониторинг и отладка

#### Логирование

```python
logger.info(f"Business message: connection={connection_id}, "
           f"chat={chat_id}, msg={message_id}")
logger.error(f"Error processing: {e}", exc_info=True)
```

#### Метрики

Отслеживайте:
- Количество подключений
- Количество обработанных сообщений
- Количество удалений/изменений
- Ошибки обработки

### Безопасность

#### Проверка подписки

```python
if not check_user_subscription(user_id):
    logger.info(f"User {user_id} has no active subscription")
    return
```

#### Ограничение размера

```python
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
if file.file_size > MAX_FILE_SIZE:
    logger.warning(f"File too large: {file.file_size}")
    return
```

#### Валидация данных

```python
def validate_message(message):
    if not message.from_user:
        return False
    if not message.chat:
        return False
    return True
```

### Производительность

#### Батчинг операций

```python
messages_batch = []
for msg in messages:
    messages_batch.append(msg)
    if len(messages_batch) >= 100:
        db.save_messages_batch(messages_batch)
        messages_batch = []
```

#### Асинхронное скачивание

```python
tasks = [download_media(file) for file in files]
await asyncio.gather(*tasks)
```

#### Кэширование

```python
from functools import lru_cache

@lru_cache(maxsize=1000)
def get_user_connection(connection_id):
    return db.get_connection(connection_id)
```

### Тестирование

```python
import pytest

@pytest.mark.asyncio
async def test_save_message():
    message = create_test_message()
    result = await process_message(message)
    assert result is not None
    
@pytest.mark.asyncio
async def test_handle_deletion():
    deleted = create_test_deletion()
    result = await process_deletion(deleted)
    assert result == True
```

### Обновления API

Telegram регулярно обновляет API. Следите за:
- https://core.telegram.org/bots/api
- https://core.telegram.org/bots/api-changelog

### Полезные ресурсы

- Официальная документация: https://core.telegram.org/bots/api
- aiogram документация: https://docs.aiogram.dev/
- Telegram Business API: https://core.telegram.org/api/bots/connected-business-bots

---

Документация актуальна на 01.03.2026
