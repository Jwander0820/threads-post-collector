import sqlite3
import datetime
import json  # 確保有引入 json 模組
from utils import iso_to_unix
import csv
import os

class SQLiteDB:
    def __init__(self, db_path="threads.db"):
        """初始化資料庫連線並建立資料表。"""
        self.db_path = db_path
        self.conn = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn:
            self.conn.close()
            print("資料庫連線已關閉")

    def initialize_tables(self):
        """
        建立 threads_posts、threads_replies 與 sync_log 資料表（若不存在則建立）。
        threads_posts 新增欄位 replies_fetched (INTEGER DEFAULT 0) 用來記錄留言是否已同步過。
        threads_replies 用來儲存留言資料。
        """
        """
        建立 threads_posts 資料表，包含以下欄位：
          - id             貼文唯一識別碼 (主鍵)
          - text           貼文文字內容
          - media_type     媒體類型 (TEXT_POST, IMAGE, VIDEO, CAROUSEL_ALBUM, REPOST_FACADE…)
          - media_url      圖片或影片 URL
          - thumbnail_url  縮圖 URL（影片用，純圖片通常為 null）
          - permalink      貼文的公開網址
          - children       複數照片/媒體資料，以 JSON 格式儲存（若沒有則為 null）
          - timestamp      貼文發佈時間（ISO8601 格式）
          - is_quote_post  是否為引用貼文 (0/1)
          - exported       是否已匯出.csv (0/1)
          - replies_fetched 是否已取得留言 (0/1)
        """
        # 建立 threads_posts 表
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS threads_posts (
                id             TEXT PRIMARY KEY,
                text           TEXT,
                media_type     TEXT,
                media_url      TEXT,
                thumbnail_url  TEXT,
                permalink      TEXT,
                children       TEXT,
                timestamp      TEXT,
                is_quote_post  INTEGER,
                exported       INTEGER DEFAULT 0,
                replies_fetched INTEGER DEFAULT 0
            );
        """)
        # 建立 threads_replies 表
        self.cur.execute("""
               CREATE TABLE IF NOT EXISTS threads_replies (
                   id                TEXT PRIMARY KEY,
                   post_id           TEXT,  -- 對應 threads_posts 的 id
                   text              TEXT,
                   username          TEXT,
                   permalink         TEXT,
                   timestamp         TEXT,
                   media_type        TEXT,
                   media_url         TEXT,
                   shortcode         TEXT,
                   thumbnail_url     TEXT,
                   children          TEXT,
                   has_replies       INTEGER,
                   root_post         TEXT,
                   replied_to        TEXT,
                   is_reply          INTEGER,
                   is_reply_owned_by_me INTEGER,
                   hide_status       TEXT,
                   FOREIGN KEY(post_id) REFERENCES threads_posts(id)
               );
           """)

        # 建立 sync_log 表
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TEXT,
                end_time TEXT,
                count INTEGER,
                initial INTEGER,
                success INTEGER
            )
        """)
        self.conn.commit()

    def insert_posts(self, posts):
        """
        將單一或多筆貼文資料插入資料庫，
        如果 posts 為 list 則依序處理；如果為 dict 則視為單筆資料處理。
        使用 INSERT OR IGNORE 避免重複插入。
        """
        if isinstance(posts, list):
            for post in posts:
                normalized_post = self._normalize_post(post)
                self._insert_single_post(normalized_post)
        elif isinstance(posts, dict):
            normalized_post = self._normalize_post(posts)
            self._insert_single_post(normalized_post)
        self.conn.commit()

    def _insert_single_post(self, normalized_post):
        sql = """
            INSERT OR IGNORE INTO threads_posts(
                id, text, media_type, media_url, thumbnail_url,
                permalink, children, timestamp, is_quote_post
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        # 將 children 轉換為 JSON 字串（如果有資料）
        children_json = json.dumps(normalized_post.get('children')) if normalized_post.get('children') is not None else None
        try:
            self.cur.execute(sql, (
                normalized_post.get('id'),
                normalized_post.get('text'),
                normalized_post.get('media_type'),
                normalized_post.get('media_url'),
                normalized_post.get('thumbnail_url'),
                normalized_post.get('permalink'),
                children_json,
                normalized_post.get('timestamp'),
                normalized_post.get('is_quote_post')
            ))
            print(f"成功插入貼文，id: {normalized_post.get('id')}")
        except Exception as e:
            print("插入資料錯誤:", e)

    def _normalize_post(self, post):
        """
        將 API 回傳的貼文資料做必要轉換：
         - 若 is_quote_post 為布林值，轉換成 1 (True) 或 0 (False)
         - 若 children 欄位存在且為 dict，取其中的 "data" 部分；否則直接保持原格式
        :param post: 原始貼文資料字典
        :return: 經過轉換後的貼文資料字典
        """
        normalized = post.copy()
        # 轉換 is_quote_post
        is_quote = normalized.get('is_quote_post')
        if isinstance(is_quote, bool):
            normalized['is_quote_post'] = 1 if is_quote else 0
        elif isinstance(is_quote, int):
            normalized['is_quote_post'] = 1 if is_quote else 0
        else:
            normalized['is_quote_post'] = 0  # 預設為 0

        # 處理 children 欄位
        children = normalized.get('children')
        if children and isinstance(children, dict) and 'data' in children:
            normalized['children'] = children['data']
        return normalized

    def _parse_time(self, time_str):
        """解析時間字串為 Unix 時間（秒）。"""
        try:
            dt = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S%z")
            return int(dt.timestamp())
        except Exception:
            try:
                return int(time_str)
            except:
                return None

    def get_max_timestamp(self):
        """取得 threads_posts 表中最大的 timestamp 值。"""
        self.cur.execute("SELECT MAX(timestamp) FROM threads_posts")
        result = self.cur.fetchone()
        if result and result[0]:
            return iso_to_unix(result[0])
        else:
            return None

    def log_sync(self, start_time, end_time, count, initial, success):
        """記錄同步日誌到 sync_log 表。"""
        self.cur.execute(
            "INSERT INTO sync_log (start_time, end_time, count, initial, success) VALUES (?, ?, ?, ?, ?)",
            (start_time, end_time, count, 1 if initial else 0, 1 if success else 0)
        )
        self.conn.commit()

    def export_to_csv(self):
        """
        將 threads_posts 表中 exported=0 的資料匯出至 CSV 檔案，並自動產生檔名：
            notiondb_import_yyyymmdd_yyyymmdd.csv
        其中最早與最新日期從 timestamp 欄位（ISO8601 字串）解析而來，
        匯出後更新這批資料的 exported 欄位為 1。

        使用方式：
            csv_filename = db.export_to_csv()
            print(f"CSV 檔案產生：{csv_filename}")
        """
        try:
            # 查詢尚未匯出的資料
            self.cur.execute("SELECT * FROM threads_posts WHERE exported=0")
            rows = self.cur.fetchall()
            if not rows:
                print("沒有未匯出的資料。")
                return

            # 取得欄位名稱（用於 CSV 檔案首行）
            colnames = [desc[0] for desc in self.cur.description]

            # 假設 timestamp 為第 8 欄 (索引 7)，時間格式為 ISO8601
            timestamps = []
            for row in rows:
                ts_str = row[7]  # 取得 timestamp 字串
                try:
                    dt = datetime.datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S%z")
                    timestamps.append(dt)
                except Exception as e:
                    print(f"解析 timestamp 失敗: {ts_str}, 錯誤: {e}")

            if not timestamps:
                print("無法解析任何 timestamp。")
                return

            earliest_dt = min(timestamps)
            latest_dt = max(timestamps)
            earliest_str = earliest_dt.strftime("%Y%m%d")
            latest_str = latest_dt.strftime("%Y%m%d")

            # 自動產生 CSV 檔案名稱
            csv_filename = f"./output/notiondb_import_{earliest_str}_{latest_str}.csv"

            # 將資料寫入 CSV 檔案（覆寫模式）
            with open(csv_filename, "w", encoding="utf-8", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(colnames)
                writer.writerows(rows)
            print(f"成功匯出 {len(rows)} 筆資料到 {csv_filename}")

            # 更新這批資料的 exported 欄位為 1，避免重複匯出
            self.cur.execute("UPDATE threads_posts SET exported=1 WHERE exported=0")
            self.conn.commit()
            return csv_filename

        except Exception as e:
            print("匯出 CSV 過程中發生錯誤:", e)

    def close(self):
        """關閉資料庫連線。"""
        self.conn.close()

    def insert_replies(self, post_id, replies):
        """
        將指定文章（post_id）的留言資料插入 threads_replies 表中。
        """
        sql = """
            INSERT OR IGNORE INTO threads_replies(
                id, post_id, text, username, permalink, timestamp,
                media_type, media_url, shortcode, thumbnail_url, children,
                has_replies, root_post, replied_to, is_reply, is_reply_owned_by_me, hide_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        for reply in replies:
            # 若 reply 裡面的 children 欄位也是 dict，可轉換為 JSON 字串
            children_json = json.dumps(reply.get('children')) if reply.get('children') is not None else None
            root_post = json.dumps(reply.get('root_post')) if reply.get('root_post') is not None else None
            replied_to = json.dumps(reply.get('replied_to')) if reply.get('replied_to') is not None else None

            try:
                self.cur.execute(sql, (
                    reply.get('id'),
                    post_id,
                    reply.get('text'),
                    reply.get('username'),
                    reply.get('permalink'),
                    reply.get('timestamp'),
                    reply.get('media_type'),
                    reply.get('media_url'),
                    reply.get('shortcode'),
                    reply.get('thumbnail_url'),
                    children_json,
                    1 if reply.get('has_replies') else 0,
                    root_post,
                    replied_to,
                    1 if reply.get('is_reply') else 0,
                    1 if reply.get('is_reply_owned_by_me') else 0,
                    reply.get('hide_status')
                ))
                print(f"成功插入留言，reply id: {reply.get('id')} (post_id: {post_id})")
            except Exception as e:
                print(f"插入留言錯誤 (reply id: {reply.get('id')}):", e)
        self.conn.commit()

    def update_replies_fetched(self, post_id):
        """
        更新指定文章(post_id)的 replies_fetched 欄位為1。
        """
        try:
            self.cur.execute("UPDATE threads_posts SET replies_fetched=1 WHERE id=?", (post_id,))
            self.conn.commit()
        except Exception as e:
            print(f"更新 replies_fetched 錯誤 (post_id: {post_id}):", e)

    def get_posts_without_replies(self):
        """
        查詢所有 media_type != 'REPOST_FACADE' 且 replies_fetched = 0 的貼文，
        用於進行留言同步。
        """
        self.cur.execute("SELECT id FROM threads_posts WHERE replies_fetched=0 AND media_type != 'REPOST_FACADE'")
        rows = self.cur.fetchall()
        return [row[0] for row in rows]
