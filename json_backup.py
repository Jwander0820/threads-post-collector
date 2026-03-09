import datetime
import json
import os


class JSONBackup:
    @staticmethod
    def backup_posts(posts, filename=None):
        """將貼文資料清單備份存成 JSON 檔案。"""
        if filename is None:
            # 預設檔名：threads_post_backup_YYYYMMDD.json（依當天日期）
            date_str = datetime.datetime.now().strftime("%Y%m%d")
            filename = f"./output/threads_post_backup_{date_str}.json"

        # 若是 output 資料夾不存在則自動建立
        directory = os.path.dirname(filename)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)

        # 將資料寫入 JSON 檔案（確保中文不轉碼）
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(posts, f, ensure_ascii=False, indent=4)
        return filename
