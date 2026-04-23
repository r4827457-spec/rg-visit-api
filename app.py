from flask import Flask, jsonify
import aiohttp
import asyncio
import json
import time
import os
from itertools import cycle
from byte import encrypt_api, Encrypt_ID
from visit_count_pb2 import Info

app = Flask(__name__)

JWT_API_URL = "https://aaall-api.vercel.app/token"
TOKEN_FILE = "tokens.json"
TOKEN_EXPIRY = 7 * 60 * 60  # 7 hours


# ================= JWT MANAGER =================
class JWTManager:
    def __init__(self):
        self.token_cache = {}
        self.load_tokens()

    def load_tokens(self):
        if os.path.exists(TOKEN_FILE):
            try:
                with open(TOKEN_FILE, "r") as f:
                    self.token_cache = json.load(f)
            except:
                self.token_cache = {}

    def save_tokens(self):
        try:
            with open(TOKEN_FILE, "w") as f:
                json.dump(self.token_cache, f)
        except Exception as e:
            print("Save error:", e)

    async def get_token(self, session, uid, password):
        now = time.time()
        uid = str(uid)

        if uid in self.token_cache:
            data = self.token_cache[uid]
            if now - data["time"] < TOKEN_EXPIRY:
                return data["token"]

        try:
            url = f"{JWT_API_URL}?uid={uid}&password={password}"

            async with session.get(url, timeout=30) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    token = data.get("token")

                    if token:
                        self.token_cache[uid] = {
                            "token": token,
                            "time": now
                        }
                        self.save_tokens()

                    return token
        except Exception as e:
            print("JWT error:", e)

        return None


jwt = JWTManager()


# ================= LOAD ACCOUNTS =================
def load_accounts():
    try:
        with open("accounts.json", "r") as f:
            data = json.load(f)
        return [acc for acc in data if "uid" in acc and "password" in acc]
    except Exception as e:
        app.logger.error(f"Account load error: {e}")
        return []


# ================= SERVER =================
def get_url(server_name):
    if server_name == "IND":
        return "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
    elif server_name in {"BR", "US", "SAC", "NA"}:
        return "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
    else:
        return "https://clientbp.ggblueshark.com/GetPlayerPersonalShow"


# ================= PARSE =================
def parse_protobuf_response(response_data):
    try:
        info = Info()
        info.ParseFromString(response_data)

        return {
            "uid": info.AccountInfo.UID or 0,
            "nickname": info.AccountInfo.PlayerNickname or "",
            "likes": info.AccountInfo.Likes or 0,
            "region": info.AccountInfo.PlayerRegion or "",
            "level": info.AccountInfo.Levels or 0
        }
    except Exception as e:
        app.logger.error(f"Protobuf error: {e}")
        return None


# ================= VISIT =================
async def visit(session, url, data, account):
    token = await jwt.get_token(session, account["uid"], account["password"])

    if not token:
        return False, None

    headers = {
        "ReleaseVersion": "OB53",
        "X-GA": "v1 1",
        "Authorization": f"Bearer {token}",
        "Host": url.replace("https://", "").split("/")[0]
    }

    try:
        async with session.post(url, headers=headers, data=data, timeout=30) as resp:
            if resp.status == 200:
                return True, await resp.read()
    except Exception as e:
        app.logger.error(f"Visit error: {e}")

    return False, None


# ================= MAIN LOOP =================
async def send_visits(target_uid, server_name, limit):
    url = get_url(server_name)

    connector = aiohttp.TCPConnector(limit=200)

    accounts = load_accounts()
    if not accounts:
        return 0, 0, None

    account_cycle = cycle(accounts)

    total_success = 0
    total_sent = 0
    player_info = None

    async with aiohttp.ClientSession(connector=connector) as session:
        encrypted = encrypt_api("08" + Encrypt_ID(str(target_uid)) + "1801")
        data = bytes.fromhex(encrypted)

        while total_success < limit:
            batch_size = min(limit - total_success, 300)

            tasks = [
                visit(session, url, data, next(account_cycle))
                for _ in range(batch_size)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            if not player_info:
                for r in results:
                    if isinstance(r, tuple) and r[0] and r[1]:
                        player_info = parse_protobuf_response(r[1])
                        break

            batch_success = sum(
                1 for r in results if isinstance(r, tuple) and r[0]
            )

            total_success += batch_success
            total_sent += batch_size

            print(f"Batch: {batch_size}, Success: {batch_success}, Total: {total_success}")

    return total_success, total_sent, player_info


# ================= API =================
@app.route('/<string:server>/<int:uid>/<int:limit>', methods=['GET'])
def api(server, uid, limit):
    server = server.upper()

    if limit > 100000:
        return jsonify({"error": "Max 100000"}), 400

    if limit <= 0:
        return jsonify({"error": "Invalid limit"}), 400

    total_success, total_sent, player_info = asyncio.run(
        send_visits(uid, server, limit)
    )

    if player_info:
        return jsonify({
            "success": total_success,
            "fail": limit - total_success,
            "uid": player_info["uid"],
            "nickname": player_info["nickname"],
            "likes": player_info["likes"],
            "level": player_info["level"],
            "region": player_info["region"]
        })
    else:
        return jsonify({"error": "Decode failed"}), 500


# ================= RUN =================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5090)