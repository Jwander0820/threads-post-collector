import datetime
import json
import os


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
