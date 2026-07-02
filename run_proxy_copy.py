import hashlib
import json
import os
import random
import sys
import time
import uuid
import smtplib
import redis
import requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.text import MIMEText

# 中国时区
CST = timezone(timedelta(hours=8))

# ====== 全局配置 ======
BASE_URL = "https://meihao.v3.api.meihaocvs.com/api"
DATA_BASE_URL = "https://meihao.v3.api.meihaocvs.com/data/api"
APP_VERSION = "4053"
PROXY_API = "https://share.proxy.qg.net/get?key=XPA2SUMF&num={num}&area=320000&distinct=true"
PROXY_USER = "XPA2SUMF"
PROXY_PASS = "FD66773E8A16"
MAX_RETRY = 3
BATCH_SIZE_MIN = 3        # 每批最少账号数
BATCH_SIZE_MAX = 8        # 每批最多账号数
RETRY_WAIT_MIN = 2        # 重试批次最小等待分钟
RETRY_WAIT_MAX = 5        # 重试批次最大等待分钟
BATCH_WAIT_MIN = 30       # 正常批次最小等待分钟
BATCH_WAIT_MAX = 60       # 正常批次最大等待分钟
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
# ====== 全局配置 ======
redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def load_config():
    with open("config.json", "r") as f:
        return json.load(f)


# Redis 连接池
def get_redis():
    return redis.Redis(connection_pool=redis_pool)


# ====== Redis 队列管理 ======

def init_queue(r, accounts):
    """初始化队列"""
    today = datetime.now(CST).strftime("%Y-%m-%d")
    pipe = r.pipeline()

    # 清空旧数据
    pipe.delete("pending", "done", "giveup", "retry_count", "queue_date", "reports")

    # 设置日期
    pipe.set("queue_date", today)

    # 添加待处理账号（score=0 表示重试次数）
    for phone in accounts:
        pipe.zadd("pending", {phone: 0})

    pipe.execute()
    print(f"队列初始化完成: {len(accounts)} 个账号")


def check_and_init_queue(r, accounts):
    """检查队列状态，必要时初始化"""
    today = datetime.now(CST).strftime("%Y-%m-%d")
    queue_date = r.get("queue_date")

    if queue_date != today:
        init_queue(r, accounts)
        return False  # 新队列

    # 检查是否还有待处理
    pending_count = r.zcard("pending")
    return pending_count > 0  # True 表示还有任务


def get_queue_status(r):
    """获取队列状态"""
    return {
        "pending": r.zcard("pending"),
        "done": r.scard("done"),
        "giveup": r.scard("giveup"),
    }


# ====== 代理管理 ======

def get_proxies(num):
    """从API获取代理"""
    try:
        url = PROXY_API.format(num=num)
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data.get("code") == "SUCCESS":
            return [item["server"] for item in data.get("data", [])]
    except Exception as e:
        print(f"获取代理失败: {e}")
    return []


# ====== 请求工具 ======

def md5(s):
    return hashlib.md5(s.encode()).hexdigest()


def make_nonce():
    rand = str(random.randint(0, 99999999)).ljust(8, '9')
    return md5("xabc" + rand)


def make_sign(data):
    substr = ''
    for key in sorted(data.keys()):
        val = data[key]
        if not val or val == '' or val == '""':
            continue
        if isinstance(val, (dict, list)):
            val = json.dumps(val, ensure_ascii=False)
            val = val.replace('"', "'").replace("true", "1").replace("false", "0")
        substr += f"{key}=|={val}&"
    substr = substr[:-1]
    substr = substr.replace(' ', '')
    return md5(substr)


def prepare_payload(data):
    payload = {**data, "app_version": APP_VERSION, "noce": make_nonce(), "timetmp": str(int(time.time() * 1000))}
    payload["sign"] = make_sign(payload)
    return payload


def post(path, data=None, auth_token=None, base_url=None, device="1", user_agent="Mozilla/5.0", proxy=None):
    headers = {
        "Content-Type": "application/json",
        "User-Agent": user_agent,
        "Device": device,
        "version": "3.0",
    }
    if auth_token:
        headers["Authorization"] = auth_token
    payload = prepare_payload(data or {})
    proxies = {"https": f"http://{PROXY_USER}:{PROXY_PASS}@{proxy}", "http": f"http://{PROXY_USER}:{PROXY_PASS}@{proxy}"} if proxy else None
    resp = requests.post(
        f"{base_url or BASE_URL}{path}",
        headers=headers,
        json=payload,
        timeout=15,
        proxies=proxies,
    )
    resp.raise_for_status()
    return resp.json()


# ====== 登录相关 ======

def get_sms_code(phone, app_uuid, user_agent="Mozilla/5.0", proxy=None):
    data = {
        "phone": phone,
        "nonce": "abc",
        "timestamp": str(int(time.time() * 1000)),
        "__Device": 2,
        "app_uuid": app_uuid,
    }
    return post("/data/getSmsCode", data, device="2", user_agent=user_agent, proxy=proxy)


def login(phone, sms_code, app_uuid, user_agent="Mozilla/5.0", proxy=None):
    data = {
        "login_type": 3,
        "phone": phone,
        "sms_code": sms_code,
        "app_uuid": app_uuid,
    }
    return post("/app/login", data, device="1", user_agent=user_agent, proxy=proxy)


# ====== 业务接口 ======

def get_sys_component_tpl(auth_token, sys_component_tpl_id="744", user_agent="Mozilla/5.0", proxy=None):
    return post("/sys/getSysComponentTpl", {"is_dev": "0", "sys_component_tpl_id": sys_component_tpl_id},
                auth_token=auth_token, user_agent=user_agent, proxy=proxy)


def get_coupon_ids_from_tpl(auth_token, sys_component_tpl_id="744", user_agent="Mozilla/5.0", proxy=None):
    tpl = get_sys_component_tpl(auth_token, sys_component_tpl_id, user_agent, proxy)
    json_data = tpl.get("data", {}).get("json_data", [])
    for item in json_data:
        if item.get("component") == "couponComponent":
            coupon_list = item.get("configs", {}).get("couponList", [])
            return [c["link"] for c in coupon_list if c.get("link")]
    return []


def list_user_label(auth_token, user_agent="Mozilla/5.0", proxy=None):
    return post("/labelV1/listUserLabel", {"user_mobile": None}, auth_token=auth_token,
                base_url=DATA_BASE_URL, user_agent=user_agent, proxy=proxy)


def get_label_json(auth_token, user_agent="Mozilla/5.0", proxy=None):
    result = list_user_label(auth_token, user_agent, proxy)
    labels = result.get("data", [])
    return [{"label_item_id": item["label_item_id"]} for item in labels if item.get("label_item_id")]


def add_user_coupon(auth_token, coupon_id_list, user_agent="Mozilla/5.0", proxy=None):
    data = {
        "coupon_id": None,
        "coupon_id_list": coupon_id_list,
        "user_mobile": None,
        "label_json": get_label_json(auth_token, user_agent, proxy),
    }
    return post("/app/addUserCoupon", data, auth_token=auth_token, user_agent=user_agent, proxy=proxy)


def get_sc_frame_user_vip(auth_token, user_agent="Mozilla/5.0", proxy=None):
    return post("/app/getScFrameUserVip", {}, auth_token=auth_token, user_agent=user_agent, proxy=proxy)


def list_extra_pay(auth_token, app_uuid, user_agent="Mozilla/5.0", proxy=None):
    data = {
        "timetmp": str(int(time.time() * 1000)),
        "app_version": APP_VERSION,
        "app_uuid": app_uuid,
    }
    return post("/app/listExtraPay", data, auth_token=auth_token, user_agent=user_agent, proxy=proxy)


def click_share(auth_token, user_vip_id, sys_component_tpl_id="744", user_agent="Mozilla/5.0", proxy=None):
    data = {
        "sys_component_tpl_id": sys_component_tpl_id,
        "user_vip_id": user_vip_id,
        "share_type": 1,
        "label_json": get_label_json(auth_token, user_agent, proxy),
    }
    return post("/operations/clickShare", data, auth_token=auth_token, user_agent=user_agent, proxy=proxy)


def list_user_lottery_coupon(auth_token, lottery_type=10, user_agent="Mozilla/5.0", proxy=None):
    return post("/lottery/listUserLotteryCoupon", {"lottery_type": lottery_type},
                auth_token=auth_token, user_agent=user_agent, proxy=proxy)


def lottery_winner(auth_token, user_coupon_id, lottery_type=10, code=100, text="优惠券", count=3,
                   user_agent="Mozilla/5.0", proxy=None):
    data = {
        "condition": {
            "code": code,
            "text": text,
            "key": f"lottery_item_type_{code}",
            "checkbox": 1,
            "value": "",
            "couponList": None,
            "count": count,
            "user_coupon_id": user_coupon_id,
        },
        "lottery_type": lottery_type,
    }
    return post("/lottery/winner", data, auth_token=auth_token, user_agent=user_agent, proxy=proxy)


# ====== 单账号完整流程 ======

def run_account(phone, user_agent="Mozilla/5.0", proxy=None):
    lines = []
    app_uuid = md5(str(uuid.uuid4()))

    def log(msg):
        print(f"[{phone}] {msg}")
        lines.append(msg)

    print(f"[{phone}] 开始登录 (代理: {proxy})...")
    sms_resp = get_sms_code(phone, app_uuid, user_agent, proxy)
    if sms_resp.get("code") != 200:
        raise Exception(f"获取验证码失败: {sms_resp}")

    sms_code = sms_resp["data"]
    login_resp = login(phone, sms_code, app_uuid, user_agent, proxy)
    if login_resp.get("code") != 200:
        raise Exception(f"登录失败: {login_resp}")

    token = login_resp["data"]["token"]
    print(f"[{phone}] 登录成功")

    vip_info = get_sc_frame_user_vip(token, user_agent, proxy)
    vip_data = vip_info.get("data", {})
    user_vip_id = vip_data.get("user_vip_id")
    user_name = vip_data.get("user_name", "")
    uniapp_device_no = vip_data.get("uniapp_device_no", "")

    log(f"账号: {phone} ({user_name})")

    coupon_ids = get_coupon_ids_from_tpl(token, "744", user_agent, proxy) + \
                get_coupon_ids_from_tpl(token, "505", user_agent, proxy)
    if coupon_ids:
        add_user_coupon(token, coupon_ids, user_agent, proxy)
        log(f"领券: {len(coupon_ids)} 张")
    else:
        log("领券: 0 张")

    if user_vip_id:
        click_share(token, user_vip_id, user_agent=user_agent, proxy=proxy)
        log("分享: 已完成")

    coupons = list_user_lottery_coupon(token, user_agent=user_agent, proxy=proxy)
    coupon_list = coupons.get("data", [])
    log(f"抽奖券: {len(coupon_list)} 张")

    for coupon in coupon_list:
        cid = coupon["user_coupon_id"]
        try:
            result = lottery_winner(token, user_coupon_id=format(cid, 'x'), user_agent=user_agent, proxy=proxy)
            data = result.get("data")
            prize = data.get("item", {}).get("item_caption", "谢谢参与") if isinstance(data, dict) and data.get(
                "item") else "谢谢参与"
            log(f"  券{cid}: {prize}")
        except Exception as e:
            log(f"  券{cid}: {e}")

    extra_pay = list_extra_pay(token, uniapp_device_no or app_uuid, user_agent, proxy)
    pay_data = extra_pay.get("data", [])
    score = next((item["cur_score"] for item in pay_data if item["pt_name"] == "会员积分"), 0)
    money = next((item["cur_money"] for item in pay_data if item["pt_name"] == "会员零钱"), 0)
    log(f"积分: {score} | 零钱: {money}")

    return "\n".join(lines)


# ====== 并发执行一批 ======

def run_batch(batch_phones, user_agents):
    """并发执行一批账号"""
    proxies = get_proxies(len(batch_phones))
    if len(proxies) < len(batch_phones):
        print(f"代理不足: 需要 {len(batch_phones)} 个，只获取到 {len(proxies)} 个")
        proxies.extend([None] * (len(batch_phones) - len(proxies)))

    results = {}

    with ThreadPoolExecutor(max_workers=len(batch_phones)) as executor:
        futures = {}
        for i, phone in enumerate(batch_phones):
            ua = random.choice(user_agents)
            proxy = proxies[i] if i < len(proxies) else None
            futures[executor.submit(run_account, phone, ua, proxy)] = phone

        for future in as_completed(futures):
            phone = futures[future]
            try:
                report = future.result(timeout=120)
                results[phone] = (report, True)
                print(f"[{phone}] 成功")
            except Exception as e:
                results[phone] = (f"[{phone}] 失败: {e}", False)
                print(f"[{phone}] 失败: {e}")

    return results


# ====== 邮件发送 ======

def send_email(subject, body, email_config):
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = email_config["sender"]
    msg["To"] = email_config["receiver"]

    with smtplib.SMTP_SSL(email_config["smtp_server"], email_config["smtp_port"]) as server:
        server.login(email_config["sender"], email_config["auth_code"])
        server.sendmail(email_config["sender"], email_config["receiver"], msg.as_string())


# ====== 主任务 ======

def daily_run():
    """执行一天的完整流程"""
    config = load_config()
    accounts = config["accounts"]
    email_config = config["email"]
    user_agents = config.get("user_agents", ["Mozilla/5.0"])

    r = get_redis()
    has_pending = check_and_init_queue(r, accounts)
    total = len(accounts)
    all_reports = []

    status = get_queue_status(r)
    print(f"[{datetime.now(CST).strftime('%Y-%m-%d %H:%M:%S')}] 开始执行")
    print(f"待处理: {status['pending']} | 已完成: {status['done']} | 已放弃: {status['giveup']}")

    while r.zcard("pending") > 0:
        # 随机取一批
        batch_size = min(random.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX), r.zcard("pending"))
        if batch_size == 0:
            break

        # 从 pending 取出（score 低的优先 = 重试次数少的）
        batch_phones = [item[0] for item in r.zrange("pending", 0, batch_size - 1, withscores=True)]
        r.zrem("pending", *batch_phones)

        print(f"\n{'='*50}")
        print(f"执行批次: {len(batch_phones)} 个账号 (剩余 {r.zcard('pending')})")
        print(f"{'='*50}")

        results = run_batch(batch_phones, user_agents)

        # 处理结果
        retry_phones = []
        success_count = 0
        fail_count = 0
        for phone in batch_phones:
            report, success = results.get(phone, (f"[{phone}] 无结果", False))

            if success:
                all_reports.append(report)  # 只把成功的加入邮件
                r.rpush("reports", report)
                r.sadd("done", phone)
                success_count += 1
            else:
                fail_count += 1
                print(f"[{phone}] 错误详情: {report}")  # 日志记录错误详情
                retry_count = r.hincrby("retry_count", phone, 1)
                if retry_count >= MAX_RETRY:
                    r.sadd("giveup", phone)
                    print(f"[{phone}] 已失败{retry_count}次，放弃")
                else:
                    retry_phones.append(phone)
                    print(f"[{phone}] 已失败{retry_count}次，放回队列重试")

        # 批次汇总
        print(f"\n批次完成: 成功 {success_count} | 失败 {fail_count}")

        # 失败的放回 pending（score=重试次数）
        if retry_phones:
            mapping = {phone: r.hget("retry_count", phone) or 0 for phone in retry_phones}
            r.zadd("pending", mapping)

        # 等待
        if r.zcard("pending") > 0:
            if retry_phones and len(retry_phones) == len(batch_phones):
                wait_min = random.randint(RETRY_WAIT_MIN, RETRY_WAIT_MAX)
            else:
                wait_min = random.randint(BATCH_WAIT_MIN, BATCH_WAIT_MAX)
            next_time = datetime.now(CST) + timedelta(minutes=wait_min)
            print(f"\n等待 {wait_min} 分钟后继续，下一批执行时间: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(wait_min * 60)

    # 汇总邮件（只包含成功的账号）
    today = datetime.now(CST).strftime("%Y-%m-%d")
    done_count = r.scard("done")
    giveup_count = r.scard("giveup")

    email_body = "\n\n".join(all_reports)
    subject = f"抽奖日报 {today} (完成{done_count}/{total})"
    if giveup_count:
        subject += f" [放弃{giveup_count}]"

    try:
        send_email(subject, email_body, email_config)
        print(f"\n邮件已发送至 {email_config['receiver']}")
    except Exception as e:
        print(f"\n邮件发送失败: {e}")

    return giveup_count == 0


def main():
    """主循环：每天0点执行"""
    if "--once" in sys.argv:
        daily_run()
        return

    if "--status" in sys.argv:
        r = get_redis()
        status = get_queue_status(r)
        print(f"待处理: {status['pending']} | 已完成: {status['done']} | 已放弃: {status['giveup']}")
        if status['pending'] > 0:
            pending = r.zrange("pending", 0, -1, withscores=True)
            for phone, retry in pending:
                print(f"  {phone} (重试{int(retry)}次)")
        return

    if "--clear" in sys.argv:
        r = get_redis()
        r.flushdb()
        print("Redis 数据已清空")
        return

    if "--add" in sys.argv:
        phones = sys.argv[sys.argv.index("--add") + 1:]
        if not phones:
            print("用法: python run_proxy.py --add 13800138000 13900139000")
            return
        r = get_redis()
        mapping = {phone: 0 for phone in phones}
        r.zadd("pending", mapping)
        print(f"已添加 {len(phones)} 个账号到队列: {', '.join(phones)}")
        return

    print("代理模式启动，每天 00:00 执行，Ctrl+C 退出")
    while True:
        now = datetime.now(CST)
        r = get_redis()
        queue_date = r.get("queue_date")
        pending_count = r.zcard("pending")

        need_run = queue_date != now.strftime("%Y-%m-%d") or pending_count > 0

        if need_run:
            print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 开始今日任务")
            daily_run()
            print("今日任务完成，等待明天...")

        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        wait_seconds = (tomorrow - now).total_seconds()
        wait_hours = int(wait_seconds // 3600)
        wait_minutes = int((wait_seconds % 3600) // 60)
        wait_secs = int(wait_seconds % 60)
        print(f"下次执行: {tomorrow.strftime('%Y-%m-%d %H:%M:%S')} (等待 {wait_hours}小时{wait_minutes}分{wait_secs}秒)")
        time.sleep(max(wait_seconds, 60))


if __name__ == "__main__":
    main()
