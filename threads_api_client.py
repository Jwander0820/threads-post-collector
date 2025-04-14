import datetime
import time

import requests

from utils import load_config, update_config_token, is_token_near_expiry


class ThreadsAPIClient:
    def __init__(self, access_token, base_url="https://graph.threads.net/v1.0"):
        """初始化 Threads API 用戶端，需提供存取權杖和基本 URL。"""
        self.access_token = access_token
        self.base_url = base_url

    def fetch_posts_paginated(self, since=None, max_retries=3):
        fields = "id,media_type,text,media_url,thumbnail_url,permalink,children,timestamp,is_quote_post"
        url = f"{self.base_url}/me/threads?limit=50&fields={fields}&access_token={self.access_token}"
        if since:
            url += f"&since={since}"
        while url:
            retries = 0
            while retries < max_retries:
                try:
                    print(f"呼叫 API：{url}")
                    response = requests.get(url)
                    response.raise_for_status()
                    data = response.json()
                    break
                except requests.exceptions.HTTPError as he:
                    retries += 1
                    print(f"HTTP error: {he}, 重試 {retries}/{max_retries} 次...")
                    time.sleep(5)  # 等待 5 秒後重試
            else:
                # 經過 max_retries 次重試後仍失敗
                raise Exception(f"API持續發生錯誤，無法取得 URL: {url}")

            posts = data.get("data", [])
            yield posts
            if "paging" in data and "next" in data["paging"]:
                url = data["paging"]["next"]
            else:
                url = None

    def fetch_posts_by_range(self, since=None, until=None):
        """
        以時間區間抓取貼文，支援 since 與 until 參數（ISO8601 時間字串）。
        當 both provided 時，僅抓取該時間段內的貼文。
        當只提供 until 時，則抓取直到此時間點以內的貼文。
        """
        fields = "id,media_type,text,media_url,thumbnail_url,permalink,children,timestamp,is_quote_post"
        url = f"{self.base_url}/me/threads?limit=50&fields={fields}&access_token={self.access_token}"
        if since:
            url += f"&since={since}"
        if until:
            url += f"&until={until}"
        posts = []
        while url:
            print(f"呼叫 API：{url}")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if "data" in data:
                posts.extend(data["data"])
            if "paging" in data and "next" in data["paging"]:
                url = data["paging"]["next"]
            else:
                url = None
        return posts

    def fetch_replies(self, post_id, max_retries=5):
        """
        針對指定文章（post_id）取得留言資料（conversation）。
        使用 API 端點：
        https://graph.threads.net/v1.0/<post_id>/conversation?fields=<想取得的資料參數>&reverse=false&access_token=<AccessToken>
        """
        fields = "id,text,username,permalink,timestamp,media_type,media_url,shortcode,thumbnail_url,children,has_replies,root_post,replied_to,is_reply,is_reply_owned_by_me,hide_status"
        url = f"{self.base_url}/{post_id}/conversation?reverse=false&fields={fields}&access_token={self.access_token}"
        retries = 0
        while retries < max_retries:
            try:
                print(f"呼叫留言 API：{url}")
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                # 根據 API 回傳結構，留言通常位於 data 內
                replies = data.get("data", [])
                return replies
            except requests.exceptions.HTTPError as he:
                retries += 1
                print(f"留言 API HTTP error: {he}, 重試 {retries}/{max_retries} 次...")
                time.sleep(3)
        raise Exception(f"取得文章 {post_id} 的留言失敗，已重試 {max_retries} 次")

    def refresh_long_lived_token(self, current_token):
        """
        使用當前長期權杖刷新取得新的權杖。
        GET /refresh_access_token
        重新整理已存在至少 24 小時但尚未過期的長期存取權杖。已重新整理的權杖有效時間為自重新整理之日起 60 天
        請求 URL 格式：
           https://graph.threads.net/refresh_access_token?grant_type=th_refresh_token&access_token=<current_token>
        回傳的 JSON 物件中會包含新的 access_token 與 expires_in。
        """
        url = f"https://graph.threads.net/refresh_access_token?grant_type=th_refresh_token&access_token={current_token}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def update_token_if_needed(self, config_path="config.json", threshold_days=30):
        """
        檢查當前存取權杖是否即將過期（剩餘天數小於 threshold_days）。
        若是，呼叫刷新 API 並更新 config.json 中的 access_token 與 expires_at。
        回傳最新的 token 與 expires_at。
        """
        config = load_config(config_path)
        current_token = config.get("access_token")
        if is_token_near_expiry(threshold_days=threshold_days, config_path=config_path):
            print("Token 即將過期，開始刷新...")
            refresh_data = self.refresh_long_lived_token(current_token)
            new_token = refresh_data.get("access_token")
            new_expires_in = int(refresh_data.get("expires_in", 0))
            if new_token and new_expires_in:
                updated_config = update_config_token(new_token, new_expires_in, config_path=config_path)
                print(
                    f"刷新成功，新 token 有效期到 {datetime.datetime.fromtimestamp(updated_config['expires_at']).isoformat()}")
                return updated_config["access_token"], updated_config["expires_at"]
            else:
                raise ValueError("刷新返回資料不完整，無法更新 token")
        else:
            print(f"Token 尚未臨近過期，無須刷新。 \n"
                  f"預計過期時間:{datetime.datetime.fromtimestamp(config.get('expires_at'))}")
            return current_token, config.get("expires_at")
