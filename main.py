from threads_api_client import ThreadsAPIClient
from sqlitedb import SQLiteDB
from json_backup import JSONBackup
from threads_sync_manger import ThreadsSyncManager
from utils import get_access_token


def initial_import(sync_manager, backup=True):
    """
    執行初始匯入所有歷史 Threads 貼文。
    備份選項 backup=True 表示同步過程的所有貼文會另存為 JSON 備份檔案。

    使用方式：
        result = initial_import(sync_manager, backup=True)
        print(f"Initial import: success={result['success']}, count={result['count']}")
    """
    result = sync_manager.initial_sync(backup=backup)
    return result


def sync_time_range(sync_manager, since, until, backup=True):
    """
    同步指定時間區段的貼文資料。
    參數：
      since: 起始時間 (ISO8601 字串, 注意必須大於等於 API 限制值)
      until: 結束時間 (ISO8601 字串)
      backup: 是否備份取得的資料到 JSON 檔案
    使用方式：
        result = sync_time_range(sync_manager, "2023-07-06T00:00:00+0000", "2024-06-01T00:00:00+0000", backup=True)
        print(f"Time range sync: success={result['success']}, count={result['count']}")
    """
    result = sync_manager.sync_time_range(since=since, until=until, backup=backup)
    return result


def sync_replies(sync_manager, backup=True):
    """
    同步 DB 中所有尚未取得留言的貼文留言。
    """
    result = sync_manager.sync_replies(backup=backup)
    return result


def incremental_update(sync_manager, backup=True):
    """
    定期更新新貼文，將從資料庫中現有最新的 timestamp 加 1 作為起點取得新貼文。
    參數：
      backup: 是否備份更新的資料到 JSON
    使用方式：
        result = incremental_update(sync_manager, backup=True)
        print(f"Incremental update: success={result['success']}, count={result['count']}")
    """
    result = sync_manager.incremental_sync(backup=backup)
    return result


# === 主程式使用示範 ===
if __name__ == "__main__":
    ACCESS_TOKEN = get_access_token()  # 預設讀取 config.json

    api_client = ThreadsAPIClient(ACCESS_TOKEN)
    backup = JSONBackup()  # 若不需要備份可設為 None
    with SQLiteDB("threads_post_backup.db") as db:
        sync_manager = ThreadsSyncManager(api_client, db, backup)

        # # 1. 初始匯入所有歷史貼文（建議首次執行）
        # result = initial_import(sync_manager, backup=True)
        # print(f"初始匯入歷史貼文: success={result['success']}, count={result['count']}")

        # 2. 同步指定時間範圍內的貼文
        # 注意：根據 API 限制，起始時間必須大於等於 API 所允許的最小 timestamp
        # result = sync_time_range(sync_manager, "2023-07-06T00:00:00+0000", "2024-06-02T00:00:00+0000", backup=True)
        # print(f"指定時間範圍內同步貼文: success={result['success']}, count={result['count']}")

        # 3. 定期更新（增量同步）新貼文
        result = sync_manager.incremental_sync(backup=True)
        print(f"更新貼文: success={result['success']}, count={result['count']}")

        # 其他功能
        # 同步留言：針對尚未同步留言的個人貼文
        replies_result = sync_replies(sync_manager, backup=True)
        print(f"同步留言: success={replies_result['success']}, count={replies_result['count']}")

        # 匯出 CSV 檔案
        csv_file = db.export_to_csv()
        if csv_file:
            print(f"CSV 檔案產生：{csv_file}")
