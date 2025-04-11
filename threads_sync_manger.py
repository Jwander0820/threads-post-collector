import datetime
from utils import iso_to_unix

class ThreadsSyncManager:
    def __init__(self, api_client, db_handler, backup_handler=None):
        """同步管理，整合 API、資料庫及備份模組。"""
        self.api = api_client
        self.db = db_handler
        self.backup = backup_handler

    def initial_sync(self, backup=False):
        """初始同步所有歷史 Threads 貼文，採用分頁處理即時寫入 DB，遇錯不中斷前面成果。"""
        start_time = datetime.datetime.now().isoformat()
        success = True
        total_count = 0
        all_posts_for_backup = []  # 備份用：累計所有抓取的貼文
        self.db.initialize_tables()
        try:
            # 使用逐頁抓取，假設 API 客戶端已改為 fetch_posts_paginated()
            for page in self.api.fetch_posts_paginated():
                if page:
                    self.db.insert_posts(page)  # 每一頁資料都立刻寫入 DB
                    total_count += len(page)
                    all_posts_for_backup.extend(page)
                    print(f"已同步 {total_count} 筆貼文...")
        except Exception as e:
            success = False
            print(f"Error during initial sync (斷點模式): {e}")
        if backup and self.backup and all_posts_for_backup:
            self.backup.backup_posts(all_posts_for_backup)
        end_time = datetime.datetime.now().isoformat()
        self.db.log_sync(start_time, end_time, total_count, initial=True, success=success)
        return {"success": success, "count": total_count}

    def incremental_sync(self, backup=False):
        """增量同步，自 DB 查詢 max timestamp 後進行新資料抓取。"""
        self.db.initialize_tables()
        start_time = datetime.datetime.now().isoformat()
        success = True
        total_count = 0
        all_posts_for_backup = []
        try:
            last_ts = self.db.get_max_timestamp()
            since = last_ts + 1 if last_ts else None
            for page in self.api.fetch_posts_paginated(since=since):
                if page:
                    self.db.insert_posts(page)
                    total_count += len(page)
                    all_posts_for_backup.extend(page)
                    print(f"增量同步：已新增 {total_count} 筆貼文...")
                else:
                    break
        except Exception as e:
            success = False
            print(f"Error during incremental sync: {e}")
        if backup and self.backup and all_posts_for_backup:
            self.backup.backup_posts(all_posts_for_backup)
        end_time = datetime.datetime.now().isoformat()
        self.db.log_sync(start_time, end_time, total_count, initial=False, success=success)
        return {"success": success, "count": total_count}

    def sync_time_range(self, since, until, backup=False):
        """
        同步指定時間區間的貼文資料（例如 "2023-07-01T00:00:00+0000" ~ "2024-07-01T00:00:00+0000"）。
        會透過 API 抓取該區段的資料，再寫入資料庫及備份 JSON（若啟用）。
        """
        start_time = datetime.datetime.now().isoformat()
        success = True
        count = 0
        try:
            # 使用新方法抓取指定區間資料
            posts = self.api.fetch_posts_by_range(since=iso_to_unix(since), until=iso_to_unix(until))
            count = len(posts)
            if backup and self.backup:
                self.backup.backup_posts(posts)
            self.db.insert_posts(posts)
        except Exception as e:
            success = False
            count = 0
            print(f"Error during time range sync: {e}")
        end_time = datetime.datetime.now().isoformat()
        self.db.log_sync(start_time, end_time, count, initial=False, success=success)
        return {"success": success, "count": count}

    def sync_replies(self, backup=False):
        """
        同步 DB 中所有尚未取得留言的貼文留言。
        僅針對 media_type != 'REPOST_FACADE' 的貼文進行。
        使用 API 呼叫 /<post_id>/conversation 取得留言。
        將留言資料存入 DB 的 thread_replies 表，並更新該貼文的 replies_fetched 為 1。

        使用方式：
            result = sync_manager.sync_replies(backup=True)
            print(f"Replies sync: success={result['success']}, count={result['count']}")
        """
        start_time = datetime.datetime.now().isoformat()
        success = True
        total_count = 0
        try:
            post_ids = self.db.get_posts_without_replies()
            for post_id in post_ids:
                try:
                    # 呼叫 API 取得留言
                    replies = self.api.fetch_replies(post_id)
                    if replies:
                        # 將留言插入 DB
                        self.db.insert_replies(post_id, replies)
                        total_count += len(replies)
                        print(f"文章 {post_id} 同步 {len(replies)} 筆留言")
                    # 無論是否有留言，更新該貼文為已同步留言
                    self.db.update_replies_fetched(post_id)
                except Exception as sub_e:
                    print(f"同步文章 {post_id} 留言失敗：{sub_e}")
        except Exception as e:
            success = False
            print(f"Error during replies sync: {e}")
        end_time = datetime.datetime.now().isoformat()
        self.db.log_sync(start_time, end_time, total_count, initial=False, success=success)
        return {"success": success, "count": total_count}
