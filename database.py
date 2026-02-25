import aiosqlite
import logging

DB_FILE = "users.db"

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY
            )
        ''')
        await db.commit()
    logging.info("База данных инициализирована.")

async def add_subscriber(chat_id: int) -> bool:
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT INTO subscribers (chat_id) VALUES (?)', (chat_id,))
            await db.commit()
        return True
    except aiosqlite.IntegrityError:
        return False

async def remove_subscriber(chat_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('DELETE FROM subscribers WHERE chat_id = ?', (chat_id,))
        await db.commit()

async def get_all_subscribers() -> list[int]:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT chat_id FROM subscribers') as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]
