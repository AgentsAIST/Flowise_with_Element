import asyncio
import aiohttp
import logging
import argparse

import uuid

import json
import time
import secrets
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional, List

import tempfile
import os

from io import BytesIO

from nio import (
    AsyncClient, MatrixRoom, RoomMessageText, RoomMessageFile,
    InviteMemberEvent, LoginError, RoomMessageImage
)

import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MIME_TO_EXTENSION = {
    'application/pdf': '.pdf',
    'text/plain': '.txt',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
    'application/json': '.json',
    'text/csv': '.csv',
    'text/markdown': '.md',
    'text/html': '.html',
    'text/css': '.css',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
}

IMAGE_MIME_TYPES = {
    'image/png', 'image/jpeg', 'image/gif',
    'image/webp', 'image/bmp', 'image/svg+xml'
}

EXTENSION_TO_MIME = {ext: mime for mime, ext in MIME_TO_EXTENSION.items()}

class FlowiseBot:

    def __init__(self, homeserver, user_id, password, flowise_url, bot_id=None):
        self.homeserver = homeserver
        self.user_id = user_id
        self.password = password
        self.flowise_url = flowise_url
        self.bot_id = bot_id

        temp_dir = tempfile.gettempdir()
        safe_user_id = user_id.replace('@', '').replace(':', '_').replace('.', '_')
        store_path = os.path.join(temp_dir, f"matrix_store_{safe_user_id}")
        os.makedirs(store_path, exist_ok=True)

        logger.info(f"📁 Store path: {store_path}")

        self.client = AsyncClient(homeserver=self.homeserver, user=self.user_id, ssl=False, store_path=store_path)

        self.start_time = int(time.time() * 1000)
        logger.info(f"⏰ Bot start time: {self.start_time}")

        self.file_cache: Dict[Tuple[str, str], List[dict]] = {}

        self.session_cache: Dict[str, str] = {}
        
        self.db_conn = None
        self._init_db_connection()
    
    def _get_db_connection(self):
        """Get database connection from environment variables"""
        return psycopg2.connect(
            host=os.getenv('DB_HOST', 'postgres'),
            database=os.getenv('DB_NAME', 'orchestrator'),
            user=os.getenv('DB_USER', 'orchestrator_user'),
            password=os.getenv('DB_PASSWORD', 'orchestrator_pass')
        )
    
    def _init_db_connection(self):
        """Initialize database connection"""
        try:
            self.db_conn = self._get_db_connection()
            logger.info("✅ Database connection established")
        except Exception as e:
            logger.warning(f"⚠️ Could not connect to database: {e}. Session persistence disabled.")
            self.db_conn = None
    
    def _get_bot_id_from_db(self) -> Optional[int]:
        """Get bot_id from database by user_id"""
        if not self.db_conn:
            return None
        try:
            cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(
                "SELECT id FROM bots WHERE bot_user_id = %s",
                (self.user_id,)
            )
            result = cursor.fetchone()
            cursor.close()
            if result:
                logger.info(f"✅ Found bot_id: {result['id']} for user: {self.user_id}")
                return result['id']
            else:
                logger.warning(f"⚠️ No bot found in DB for user: {self.user_id}")
                return None
        except Exception as e:
            logger.error(f"❌ Error getting bot_id from DB: {e}")
            return None
    
    def _get_session_from_db(self, user_id: str, room_id: str) -> Optional[str]:
        """Get session_id from database for given user and room"""
        if not self.db_conn or not self.bot_id:
            return None
        try:
            cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(
                "SELECT session_id FROM sessions WHERE bot_id = %s AND user_id = %s AND room_id = %s",
                (self.bot_id, user_id, room_id)
            )
            result = cursor.fetchone()
            cursor.close()
            if result and result['session_id']:
                logger.debug(f"📝 Retrieved session from DB for room {room_id[:20]}...")
                return result['session_id']
            return None
        except Exception as e:
            logger.error(f"❌ Error getting session from DB: {e}")
            return None
    
    def _save_session_to_db(self, user_id: str, room_id: str, session_id: str) -> bool:
        """Save or update session_id in database"""
        if not self.db_conn or not self.bot_id:
            return False
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                INSERT INTO sessions (bot_id, user_id, room_id, session_id, updated_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (bot_id, user_id, room_id) 
                DO UPDATE SET session_id = EXCLUDED.session_id, updated_at = CURRENT_TIMESTAMP
            """, (self.bot_id, user_id, room_id, session_id))
            self.db_conn.commit()
            cursor.close()
            logger.debug(f"💾 Saved session to DB for room {room_id[:20]}...")
            return True
        except Exception as e:
            logger.error(f"❌ Error saving session to DB: {e}")
            if self.db_conn:
                self.db_conn.rollback()
            return False
    
    def _reset_session_in_db(self, user_id: str, room_id: str, new_session_id: str) -> bool:
        """Reset session_id in database"""
        if not self.db_conn or not self.bot_id:
            return False
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                UPDATE sessions 
                SET session_id = %s, updated_at = CURRENT_TIMESTAMP
                WHERE bot_id = %s AND user_id = %s AND room_id = %s
            """, (new_session_id, self.bot_id, user_id, room_id))
            self.db_conn.commit()
            cursor.close()
            logger.info(f"🔄 Reset session in DB for room {room_id[:20]}...")
            return True
        except Exception as e:
            logger.error(f"❌ Error resetting session in DB: {e}")
            if self.db_conn:
                self.db_conn.rollback()
            return False

    def should_process_message(self, event) -> bool:
        event_source = getattr(event, 'source', {})
        content = event_source.get('content', {})
        event_ts = event_source.get('origin_server_ts', 0)

        if event_ts == 0:
            logger.debug("❓ Message has no timestamp, processing anyway")
            return True

        if event_ts < self.start_time:
            logger.debug(f"⏭️ Skipping old message (event ts: {event_ts} < bot start ts: {self.start_time})")
            return False

        return True

    async def login_with_retry(self, retries=3):
        for attempt in range(retries):
            try:
                logger.info(f"🔐 Login attempt {attempt + 1}/{retries}...")

                login_response = await self.client.login(self.password)

                if isinstance(login_response, LoginError):
                    logger.error(f"❌ Login failed: {login_response.message}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    else:
                        raise Exception(f"Login failed after {retries} attempts: {login_response.message}")

                logger.info(f"✅ Login successful! User: {self.client.user_id}, Device: {self.client.device_id}")
                return True

            except Exception as e:
                logger.error(f"❌ Login error (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2**attempt)
                else:
                    raise

        return False

    @staticmethod
    def generate_random_session_id() -> str:
        return str(uuid.uuid4())

    def get_or_create_session(self, room_id: str) -> str:
        # First check if we have it in memory cache
        if room_id in self.session_cache:
            return self.session_cache[room_id]
        
        # Try to get from database
        db_session = self._get_session_from_db(self.user_id, room_id)
        if db_session:
            self.session_cache[room_id] = db_session
            logger.info(f"📝 Restored session from DB for room {room_id[:20]}...: {db_session}")
            return db_session
        
        # Create new session
        session_id = self.generate_random_session_id()
        self.session_cache[room_id] = session_id
        self._save_session_to_db(self.user_id, room_id, session_id)
        logger.info(f"📝 Created new session for room {room_id[:20]}...: {session_id}")

        return session_id

    def reset_session(self, room_id: str) -> str:
        old_session = self.session_cache.get(room_id, "no session")
        session_id = self.generate_random_session_id()
        self.session_cache[room_id] = session_id

        keys_to_remove = [key for key in self.file_cache.keys() if key[0] == room_id]
        for key in keys_to_remove:
            del self.file_cache[key]
        
        # Update session in database
        self._reset_session_in_db(self.user_id, room_id, session_id)
        
        logger.info(f"🔄 Reset session for room {room_id[:20]}...")
        return session_id

    async def on_invite(self, room: MatrixRoom, event: InviteMemberEvent) -> None:
        if event.state_key == self.user_id:
            logger.info(
                f"🤝 Accepting invitation to room {room.room_id[:20]}...")
            try:
                await self.client.join(room.room_id)
                logger.info(f"✅ Joined room: {room.room_id[:20]}...")

                self.get_or_create_session(room.room_id)
            except Exception as e:
                logger.error(f"❌ Failed to join room {room.room_id[:20]}: {e}")

    async def send_unencrypted_message(self, room_id: str, text: str):
        try:
            url = f"{self.homeserver}/_matrix/client/v3/rooms/{room_id}/send/m.room.message"

            headers = {
                "Authorization": f"Bearer {self.client.access_token}",
                "Content-Type": "application/json"
            }

            data = {"msgtype": "m.text", "body": text}

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data,
                                        headers=headers) as response:
                    if response.status == 200:
                        logger.info("✅ Sent unencrypted message")
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Failed to send unencrypted message: {response.status} - {error_text}")

        except Exception as e:
            logger.error(f"❌ Error sending unencrypted message: {e}")

    async def upload_file_to_flowise(self, file_bytes: bytes, filename: str, mime_type: str, chat_id: str) -> str:
        url = self.flowise_url.replace('/prediction/', '/attachments/') + '/' + chat_id

        form = aiohttp.FormData()

        file_obj = BytesIO(file_bytes)

        form.add_field(
            'files',
            file_obj,
            filename=filename,
            content_type=mime_type
        )

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                        url, data=form,
                        timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Flowise attachments error {response.status}: {error_text}")
                        raise Exception(f"Flowise attachments error: {response.status}")

                    file_info_list = await response.json()
                    if not file_info_list or not isinstance(file_info_list, list):
                        raise Exception("Invalid response from Flowise attachments API")

                    file_info = file_info_list[0]
                    extracted_text = file_info.get('content', '').strip()

                    if not extracted_text:
                        logger.warning("⚠️ Flowise returned empty content for file")

                        extracted_text = f"[Содержимое файла '{filename}' не было извлечено автоматически]"

                    logger.info(f"✅ Flowise extracted text ({len(extracted_text)} symbols) from '{filename}'")
                    return extracted_text

        except asyncio.TimeoutError:
            logger.error("⏰ Flowise attachments request timeout")
            raise Exception("Flowise не ответил вовремя при загрузке файла")
        except Exception as e:
            logger.error(f"💥 Error during file upload to Flowise: {e}")
            raise Exception

    async def download_file_bytes(self, mxc_url: str) -> Optional[bytes]:
        try:
            logger.info(f"⬇️ Downloading file bytes: {mxc_url}")

            response = await self.client.download(mxc_url)
            if response and hasattr(response, 'body'):
                if len(response.body) > 100 * 1024 * 1024:
                    logger.warning(
                        f"File too large: {len(response.body)} bytes")
                    return None

                logger.info(f"✅ Downloaded file: {len(response.body)} bytes")
                return response.body

            logger.error(f"Failed to download file from {mxc_url}")
            return None
        except Exception as e:
            logger.error(f"Error downloading file bytes: {e}")
            import traceback
            traceback.print_exc()
            return None

    @staticmethod
    def bytes_to_base64_data_url(file_bytes: bytes, mime_type: str) -> str:
        import base64
        b64 = base64.b64encode(file_bytes).decode('utf-8')
        return f"data:{mime_type};base64,{b64}"

    def is_image_mime(self, mime_type: str) -> bool:
        return mime_type.lower() in IMAGE_MIME_TYPES

    @staticmethod
    def detect_mime_type(event, file_name: str, logger) -> tuple[str, int, str]:
        mime_type = 'application/octet-stream'
        file_size = 0
        method = "unknown"

        if hasattr(event, 'file') and event.file:
            if hasattr(event.file, 'mimetype') and event.file.mimetype:
                mime_type = event.file.mimetype
                method = "event.file.mimetype"
            if hasattr(event.file, 'size'):
                file_size = event.file.size

        if mime_type == 'application/octet-stream' and hasattr(event, 'source'):
            source_content = event.source.get('content', {})
            info = source_content.get('info', {}) if isinstance(source_content, dict) else {}
            if isinstance(info, dict):
                if info.get('mimetype'):
                    mime_type = info['mimetype']
                    method = "source.info.mimetype"
                if info.get('size'):
                    file_size = info['size']

        if mime_type == 'application/octet-stream' and '.' in file_name:
            ext = '.' + file_name.split('.')[-1].lower()
            if ext in EXTENSION_TO_MIME:
                mime_type = EXTENSION_TO_MIME[ext]
                method = f"extension_fallback:{ext}"
                logger.info(
                    f"🔄 MIME determined from extension: {ext} → {mime_type}")

        return mime_type, file_size, method

    async def on_file(self, room: MatrixRoom, event) -> None:
        if event.sender == self.client.user_id:
            return

        if not self.should_process_message(event):
            return

        is_image = isinstance(event, RoomMessageImage)
        media_label = "🖼️ Image" if is_image else "📄 File"
        logger.info(f"{media_label} from {event.sender}: {event.body}")

        try:
            file_name = event.body or ('image.jpg' if is_image else 'file')
            original_name = file_name

            mime_type, file_size, detection_method = self.detect_mime_type(event, file_name, logger)

            if '.' not in file_name and mime_type in MIME_TO_EXTENSION:
                if mime_type in MIME_TO_EXTENSION:
                    file_name += MIME_TO_EXTENSION[mime_type]
                elif is_image and mime_type in IMAGE_MIME_TYPES:
                    file_name += IMAGE_MIME_TYPES[mime_type]
                logger.debug(f"✏️ Added extension: {file_name}")

            logger.info(f"📦 File: '{original_name}' → '{file_name}' | MIME: {mime_type} | Size: {file_size}B | Method: {detection_method}")

            supported_types = list(
                MIME_TO_EXTENSION.keys()) + list(IMAGE_MIME_TYPES)

            if mime_type not in supported_types:
                logger.warning(f"⚠️ Unsupported file type: {mime_type}")
                await self.send_text_message(
                    room.room_id,
                    f"Формат файла {mime_type} не поддерживается.")
                return

            if hasattr(event, 'url'):
                file_bytes = await self.download_file_bytes(event.url)
                if file_bytes:
                    cache_key = (room.room_id, event.sender)
                    
                    if cache_key not in self.file_cache:
                        self.file_cache[cache_key] = []

                    file_entry = {
                        'bytes': file_bytes,
                        'mime': mime_type,
                        'name': file_name,
                        'size': file_size,
                        'is_image': self.is_image_mime(mime_type)
                    }
                    self.file_cache[cache_key].append(file_entry)

                    logger.info(f"💾 Cached file '{file_name}' (#{len(self.file_cache[cache_key])}) for {event.sender}")

                    file_type = "🖼️ Изображение" if file_entry['is_image'] else "📄 Документ"
                    size_info = f" ({file_size} байт)" if file_size > 0 else ""
                    await self.send_text_message(
                        room.room_id,
                        f"{file_type} '{file_name}' получен{size_info}. "
                        f"Загружено файлов: {len(self.file_cache[cache_key])}. "
                        f"Задайте вопрос или используйте !rag для документов."
                    )
                else:
                    await self.send_text_message(
                        room.room_id,
                        f"Не удалось загрузить файл '{file_name}'. Возможно, он слишком большой (>100MB)."
                    )

        except Exception as e:
            logger.error(f"💥 Error processing file: {e}")
            import traceback
            traceback.print_exc()
            await self.send_text_message(
                room.room_id, f"Ошибка при обработке файла: {str(e)[:100]}")

    async def send_text_message(self, room_id: str, text: str):
        content = {"msgtype": "m.text", "body": text}
        await self.safe_room_send(room_id, content)

    async def safe_room_send(self, room_id: str, content: dict, max_retries=3):
        for attempt in range(max_retries):
            try:
                await self.client.room_send(room_id=room_id, message_type="m.room.message", content=content)
                return True

            except KeyError as e:
                logger.warning(f"⚠️ Attempt {attempt+1}/{max_retries}: KeyError, retrying...: {e}")
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"❌ Attempt {attempt+1}/{max_retries}: Unexpected error: {e}")
                break

        logger.error(
            f"❌ All {max_retries} attempts failed, trying HTTP API...")
        try:
            await self.send_unencrypted_message(
                room_id, content.get('body', 'Message failed'))
            return True
        except Exception as e:
            logger.error(f"❌ HTTP API also failed: {e}")
            return False

    async def on_message(self, room: MatrixRoom, event: RoomMessageText) -> None:
        if event.sender == self.client.user_id:
            return

        if not self.should_process_message(event):
            return

        logger.info(f"📨 Message from {event.sender}: {event.body[:100]}...")

        if event.body.startswith('!'):
            await self.handle_command(room, event)
            return

        cache_key = (room.room_id, event.sender)
        cached_files = self.file_cache.pop(cache_key, [])

        session_id = self.get_or_create_session(room.room_id)

        try:
            payload = {
                "question": event.body,
                "overrideConfig":
                    {
                        "chatId": session_id,
                        "sessionId": session_id
                    }
            }

            images = [f for f in cached_files if f['is_image']]
            documents = [f for f in cached_files if not f['is_image']]

            if images:
                uploads = []
                for img in images:
                    try:
                        data_url = self.bytes_to_base64_data_url(img['bytes'], img['mime'])
                        uploads.append({
                            "data": data_url,
                            "type": "file",
                            "name": img['name'],
                            "mime": img['mime']
                        })
                        logger.info(f"🖼️ Processed image: {img['name']} ({len(img['bytes'])} bytes)")
                    except Exception as e:
                        logger.error(f"❌ Convertation image error {img['name']}: {e}")
                
                if uploads:
                    payload["uploads"] = uploads
                    logger.info(f"📤 Adding {len(uploads)} images to upload")

            if documents:
                doc_texts = []
                for doc in documents:
                    try:
                        extracted = await self.upload_file_to_flowise(
                            file_bytes=doc['bytes'],
                            filename=doc['name'],
                            mime_type=doc['mime'],
                            chat_id=session_id
                        )
                        doc_texts.append(f"📄 {doc['name']}:\n{extracted}")
                    except Exception as e:
                        logger.warning(f"⚠️ Can't process file {doc['name']}: {e}")
                        doc_texts.append(f"⚠️ File '{doc['name']}' can't read")
                
                if doc_texts:
                    payload["question"] = (
                        f"Вопрос: {event.body}\n\n" + 
                        "\n\n".join(doc_texts)
                    )
                    logger.info(f"📄 Adding text from {len(doc_texts)} docs")

            timeout = aiohttp.ClientTimeout(total=300)

            async with aiohttp.ClientSession() as session:
                async with session.post(self.flowise_url, json=payload, timeout=timeout) as response:
                    if response.status == 200:
                        result = await response.json()
                        answer = result.get('text', 'No response from Flowise')
                    elif response.status == 413:
                        answer = "Файл(ы) слишком большие для обработки."
                    else:
                        error_text = await response.text()
                        logger.error(f"Flowise error {response.status}: {error_text[:500]}")
                        answer = f"Ошибка Flowise: {response.status}"

            await self.send_text_message(room.room_id, answer)
            logger.info(f"📤 Response to user {event.sender}")

        except asyncio.TimeoutError:
            logger.error("⏰ Flowise request timeout")
            await self.send_text_message(
                room.room_id, "Flowise не ответил вовремя. Попробуйте позже.")
        except Exception as e:
            logger.error(f"💥 Error at user message processing {e}")
            import traceback
            traceback.print_exc()
            error_msg = f"Ошибка: {str(e)[:300]}"
            await self.send_text_message(room.room_id, error_msg)

    async def _upload_single_file_to_rag(self, room_id: str, file_entry: dict, session_id: str, chunk_size: int, chunk_overlap: int) -> Tuple[bool, str]:
        file_name = file_entry['name']
        file_bytes = file_entry['bytes']
        mime_type = file_entry['mime']
        
        if file_entry.get('is_image', False):
            return False, f"🖼️ '{file_name}' — изображения не поддерживаются для RAG (только для визуального QA)"
        
        try:
            API_URL = self.flowise_url.replace('/prediction/', '/vector/upsert/')
            logger.info(f"📤 RAG upsert: '{file_name}' → {API_URL}")

            form = aiohttp.FormData()
            form.add_field('chatId', session_id)
            form.add_field('sessionId', session_id)
            form.add_field('files', file_bytes, filename=file_name, content_type=mime_type)
            form.add_field('chunkSize', str(chunk_size))
            form.add_field('chunkOverlap', str(chunk_overlap))

            headers = {"Accept": "application/json"}
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    API_URL,
                    data=form,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as response:
                    response_text = await response.text()
                    
                    if response.status == 200:
                        try:
                            result = json.loads(response_text)
                            added = result.get('numAdded', 0)
                            updated = result.get('numUpdated', 0)
                            
                            if added > 0 or updated > 0:
                                msg = f"✅ '{file_name}': +{added} чанков"
                                if updated > 0:
                                    msg += f", ~{updated} обновлено"
                                return True, msg
                            else:
                                return True, f"⚪ '{file_name}': контент уже в базе (0 новых чанков)"
                                
                        except json.JSONDecodeError:
                            logger.warning(f"⚠️ Can't parse response for '{file_name}': {response_text[:100]}")
                            return True, f"✅ '{file_name}': загружен (ответ не в JSON)"
                    else:
                        logger.error(f"❌ RAG error {response.status} for '{file_name}': {response_text[:200]}")
                        return False, f"❌ '{file_name}': ошибка {response.status}"
                        
        except asyncio.TimeoutError:
            logger.error(f"⏰ Timeout при загрузке '{file_name}'")
            return False, f"⏰ '{file_name}': таймаут"
        except Exception as e:
            logger.error(f"💥 Ошибка загрузки '{file_name}': {e}")
            return False, f"❌ '{file_name}': {str(e)[:50]}"

    async def _process_rag_for_files(self, room_id: str, sender: str, session_id: str, chunk_size: int, chunk_overlap: int) -> None:
        cache_key = (room_id, sender)
        cached_files = self.file_cache.get(cache_key, [])
        
        if not cached_files:
            await self.send_text_message(
                room_id,
                "❌ Нет файлов для загрузки в RAG.\n\n"
                "1. Отправьте файл(ы) в чат (PDF, DOCX, TXT, CSV и т.д.)\n"
                "2. Выполните команду: `!rag [chunkSize=300] [chunkOverlap=150]`"
            )
            return
        
        documents = [f for f in cached_files if not f.get('is_image', False)]
        images = [f for f in cached_files if f.get('is_image', False)]
        
        if not documents:
            await self.send_text_message(
                room_id,
                "🖼️ В кэше только изображения.\n"
                "Для RAG отправьте текстовые файлы"
            )
            _ = self.file_cache.pop(cache_key, None)
            return
        
        progress_msg = (
            f"Обработка {len(documents)} файл(ов) для RAG...\n"
            f"Настройки: chunk={chunk_size}, overlap={chunk_overlap}"
        )
        if images:
            progress_msg += f"\n\n🖼️ {len(images)} изображение(ий) пропущено (не для RAG)"
        
        await self.send_text_message(room_id, progress_msg)
        
        results = []
        for idx, doc in enumerate(documents, 1):
            if len(documents) > 1:
                await self.send_text_message(
                    room_id,
                    f"⏳ [{idx}/{len(documents)}] Обрабатываю: {doc['name']}..."
                )
            
            success, message = await self._upload_single_file_to_rag(
                room_id=room_id,
                file_entry=doc,
                session_id=session_id,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap
            )
            results.append((doc['name'], success, message))

            if idx < len(documents):
                await asyncio.sleep(1)

        success_count = sum(1 for _, ok, _ in results if ok)
        fail_count = len(results) - success_count
        
        report_lines = [f"\n📋 Итог обработки RAG ({success_count}✅ / {fail_count}❌):"]
        for name, success, msg in results:
            report_lines.append(f"{msg}")
        
        if success_count > 0:
            report_lines.append(f"Готово! Теперь можно задавать вопросы по загруженным документам.")
        
        await self.send_text_message(room_id, "\n".join(report_lines))

        _ = self.file_cache.pop(cache_key, None)
        logger.info(f"🧹 Clear cache for files for user {sender} in room {room_id}")

    async def handle_command(self, room: MatrixRoom, event: RoomMessageText):
        command = event.body.strip()
        
        if command.startswith('!rag'):
            args = command.split()
            chunk_size = 300
            chunk_overlap = 150

            for arg in args[1:]:
                if '=' in arg:
                    key, value = arg.split('=', 1)
                    if key == 'chunkSize':
                        try:
                            chunk_size = int(value)
                        except ValueError:
                            await self.send_text_message(
                                room.room_id,
                                f"❌ Неверное значение chunkSize: {value}"
                            )
                            return
                    elif key == 'chunkOverlap':
                        try:
                            chunk_overlap = int(value)
                        except ValueError:
                            await self.send_text_message(
                                room.room_id,
                                f"❌ Неверное значение chunkOverlap: {value}"
                            )
                            return
            
            session_id = self.get_or_create_session(room.room_id)
            await self._process_rag_for_files(
                room_id=room.room_id,
                sender=event.sender,
                session_id=session_id,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap
            )
            return

        elif command == "!reset":
            new_session_id = self.reset_session(room.room_id)
            self.file_cache = {}
            await self.send_text_message(
                room.room_id,
                f"Сессия сброшена. Начинаем новый диалог.\nНовая сессия: {new_session_id}"
            )

        elif command == "!session":
            session_id = self.get_or_create_session(room.room_id)
            await self.send_text_message(
                room.room_id,
                f"ID текущей сессии: {session_id}\nКомната: {room.room_id[:30]}..."
            )

        elif command == "!help" or command == "!start":
            help_text = """Команды бота:
!help или !start - Показать это сообщение
!reset - Сбросить историю диалога (начать новый разговор)
!session - Показать ID текущей сессии
!rag [chunkSize=300] [chunkOverlap=150] [metadata="{}"] - Загрузить файл в базу данных

Как отправить файл:
1. Просто отправьте файл в чат (PDF, TXT, DOCX, изображения)
2. Бот подтвердит получение файла
3. Задайте вопрос по файлу

Лимит файла: ~10MB
Сессии: Каждая комната имеет свою сессию, бот помнит контекст в рамках комнаты"""

            await self.send_text_message(room.room_id, help_text)

        elif command == "!status":
            total_files = sum(len(files) for files in self.file_cache.values())
            status_text = f"""Статус бота:
Пользователь: {self.client.user_id}
Активные сессии: {len(self.session_cache)}
Файлов в кэше: {total_files} (в {len(self.file_cache)} очередях)
Flowise: {self.flowise_url}
Время запуска: {datetime.fromtimestamp(self.start_time/1000, timezone.utc)}"""
            await self.send_text_message(room.room_id, status_text)

        else:
            await self.send_text_message(
                room.room_id,
                f"Неизвестная команда: {command}\nИспользуйте !help для списка команд."
            )

    async def run(self):
        try:
            logger.info(f"Starting Flowise Matrix Bot {self.user_id}...")
            logger.info(f"Homeserver: {self.homeserver}")
            logger.info(f"Flowise URL: {self.flowise_url}")
            logger.info(f"Bot ID: {self.bot_id}")
            logger.info(f"Filter messages newer than: {datetime.fromtimestamp(self.start_time/1000, timezone.utc)}")

            if not await self.login_with_retry():
                logger.error("Failed to login after all retries")
                return

            if not self.client.user_id or not self.client.access_token:
                logger.error("❌ Not properly logged in. Missing user_id or access_token")
                return

            logger.info(f"✅ Logged in as {self.client.user_id}")
            
            # If bot_id was not provided, try to get it from database
            if not self.bot_id:
                self.bot_id = self._get_bot_id_from_db()
                if self.bot_id:
                    logger.info(f"✅ Retrieved bot_id from DB: {self.bot_id}")
                else:
                    logger.warning("⚠️ Running without bot_id - session persistence will be disabled")

            self.client.add_event_callback(self.on_invite, InviteMemberEvent)
            self.client.add_event_callback(self.on_message, RoomMessageText)
            self.client.add_event_callback(self.on_file, RoomMessageFile)
            self.client.add_event_callback(self.on_file, RoomMessageImage)

            logger.info("🔄 Starting initial sync...")
            sync_response = await self.client.sync(timeout=30000)
            if sync_response:
                logger.info(f"✅ Initial sync completed. Next batch: {sync_response.next_batch[:20]}...")
            else:
                logger.warning("⚠️ Initial sync returned empty response")

            logger.info("👂 Bot is ready and listening for messages and files...")
            logger.info("📁 Supported file types: PDF, TXT, DOCX, Excel, JSON, CSV, images, code")
            logger.info("💬 Commands: !help, !reset, !session, !status")
            logger.info("💾 Session persistence: " + ("enabled" if self.db_conn and self.bot_id else "disabled"))

            await self.client.sync_forever(timeout=30000)

        except Exception as e:
            logger.error(f"💀 Fatal error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.db_conn:
                try:
                    self.db_conn.close()
                    logger.info("🔒 Database connection closed")
                except Exception as e:
                    logger.warning(f"⚠️ Error closing DB connection: {e}")
            if self.client:
                await self.client.close()
            logger.info("👋 Bot stopped")

async def main():
    parser = argparse.ArgumentParser(description='Matrix Flowise Bot')
    parser.add_argument('--homeserver', required=True, help='Matrix homeserver URL')
    parser.add_argument('--user', required=True, help='Bot user ID (e.g., @bot:localhost)')
    parser.add_argument('--password', required=True, help='Bot password')
    parser.add_argument('--flowise-url', required=True, help='Flowise API URL')
    parser.add_argument('--bot-id', type=int, default=None, help='Bot ID from database (optional)')

    args = parser.parse_args()

    bot = FlowiseBot(
        homeserver=args.homeserver,
        user_id=args.user,
        password=args.password,
        flowise_url=args.flowise_url,
        bot_id=args.bot_id
    )
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
