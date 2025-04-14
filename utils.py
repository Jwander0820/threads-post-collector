import datetime
import json
import os
import time


def iso_to_unix(iso_str):
    dt = datetime.datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S%z")
    return int(dt.timestamp())


def load_config(config_path="config.json"):
    """
    從指定的 JSON 配置檔案讀取配置。
    若檔案不存在，則會拋出 FileNotFoundError。
    回傳值為一個 dict，包含配置檔中所有的鍵值資料。
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file {config_path} not found.")
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config


def get_access_token(config_path="config.json"):
    """
    從配置檔中取得 access_token。
    此函數假設 config.json 中有 "access_token" 這個鍵值。
    如無法取得將會拋出 ValueError。
    """
    config = load_config(config_path)
    token = config.get("access_token")
    if not token:
        raise ValueError("access_token not found in config file.")
    return token


def update_config_token(new_token, new_expires_in, config_path="config.json"):
    """
    根據刷新 API 返回的資料，更新 config.json 中的 access_token 與 expires_at。
    new_expires_in 是從 API 回傳的秒數（例如 5184000 秒約為60天）。
    expires_at 儲存為 Unix timestamp。
    """
    config = load_config(config_path)
    current_ts = int(time.time())
    # 計算新的過期時間
    new_expires_at = current_ts + new_expires_in
    config["access_token"] = new_token
    config["expires_at"] = new_expires_at
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)
    return config


def is_token_near_expiry(threshold_days=7, config_path="config.json"):
    """
    檢查存於 config.json 中的權杖是否即將過期：
      - 若 config 中含有 expires_at 欄位，則計算剩餘秒數。
      - 如果剩餘時間小於 threshold_days（預設7天）則回傳 True。
      - 否則回傳 False。
    """
    config = load_config(config_path)
    expires_at = config.get("expires_at")
    if not expires_at:
        # 若沒有 expires_at 記錄，則視為需要刷新
        return True
    current_ts = int(time.time())
    remaining = int(expires_at) - current_ts
    return remaining < threshold_days * 24 * 3600
