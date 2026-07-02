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

# 时间窗口配置
DAY_START_HOUR = 6       # 每天开始执行时间
DAY_END_HOUR = 23        # 正常执行结束时间（小时）
DAY_END_MINUTE = 30      # 正常执行结束时间（分钟）
RETRY_END_HOUR = 2       # 重试缓冲区结束时间（次日凌晨）
BATCH_SIZE_MIN = 3       # 每批最少账号数
BATCH_SIZE_MAX = 8       # 每批最多账号数
TICK_INTERVAL = 30       # 心跳间隔（秒）

# Redis 配置
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))

# Redis 连接池
redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def get_redis():
    return redis.Redis(connection_pool=redis_pool)


def load_config():
    with open("config.json", "r") as f:
        return json.load(f)


# ====== 时间工具 ======

def now_cst():
    return datetime.now(CST)


def today_str():
    return now_cst().strftime("%Y-%m-%d")


def get_day_window():
    """获取正常执行时间窗口 (06:00 - 23:30)"""
    now = now_cst()
    start = now.replace(hour=DAY_START_HOUR, minute=0, second=0, microsecond=0)
    end = now.replace(hour=DAY_END_HOUR, minute=DAY_END_MINUTE, second=0, microsecond=0)
    return start, end


def get_retry_window():
    """获取重试缓冲区时间窗口 (23:30 - 次日02:00)"""
    now = now_cst()
    start = now.replace(hour=DAY_END_HOUR, minute=DAY_END_MINUTE, second=0, microsecond=0)
    end = (now + timedelta(days=1)).replace(hour=RETRY_END_HOUR, minute=0, second=0, microsecond=0)
    return start, end


def is_in_sleep_window():
    """检查是否在睡眠窗口 (02:00 - 06:00)"""
    hour = now_cst().hour
    return 2 <= hour < 6


# ====== Redis 队列管理 ======

def init_queue(accounts):
    """初始化队列"""
    r = get_redis()
    today = today_str()
    pipe = r.pipeline()

    # 清空旧数据
    pipe.delete("pending", "done", "giveup", "retry_count", "retry_queue", "queue_date", "reports", "schedule")

    # 设置日期
    pipe.set("queue_date", today)

    # 添加待处理账号
    for phone in accounts:
        pipe.zadd("pending", {phone: 0})

    pipe.execute()
    print(f"队列初始化完成: {len(accounts)} 个账号")


def check_queue_date():
    """检查队列日期，必要时重置"""
    r = get_redis()
    queue_date = r.get("queue_date")
    if queue_date != today_str():
        return False
    return True


def get_queue_status():
    r = get_redis()
    return {
        "pending": r.zcard("pending"),
        "done": r.scard("done"),
        "giveup": r.scard("giveup"),
        "retry": r.zcard("retry_queue"),
        "schedule": r.zcard("schedule"),
    }


# ====== 调度器 ======

def generate_schedule():
    """生成今日调度表"""
    r = get_redis()
    config = load_config()
    accounts = config["accounts"]

    # 初始化队列
    init_queue(accounts)

    # 获取所有待处理账号
    pending = r.zrange("pending", 0, -1)
    if not pending:
        print("没有待处理账号")
        return

    # 随机打乱
    random.shuffle(pending)

    # 切分成批次
    batches = []
    i = 0
    while i < len(pending):
        size = random.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)
        batch = pending[i:i + size]
        batches.append(batch)
        i += size

    # 获取时间窗口
    start, end = get_day_window()
    total_minutes = int((end - start).total_seconds() / 60)
    interval = total_minutes // len(batches)

    # 生成时间点并写入 schedule
    pipe = r.pipeline()
    for idx, batch in enumerate(batches):
        # 在每个区间内随机取一个时间点
        slot_start = start + timedelta(minutes=idx * interval)
        slot_end = start + timedelta(minutes=(idx + 1) * interval)
        random_minutes = random.randint(0, int((slot_end - slot_start).total_seconds() / 60))
        execute_time = slot_start + timedelta(minutes=random_minutes)

        # 存入 schedule ZSET (score=时间戳, member=批次数据)
        batch_data = json.dumps({
            "phones": batch,
            "batch_no": idx + 1,
            "created": now_cst().isoformat(),
        })
        pipe.zadd("schedule", {batch_data: execute_time.timestamp()})

        print(f"批次 {idx + 1}: {len(batch)} 个账号, 执行时间 {execute_time.strftime('%H:%M')}")

    pipe.execute()
    print(f"\n调度表生成完成: {len(batches)} 个批次")


def generate_retry_schedule():
    """生成重试调度表"""
    r = get_redis()
    retry_phones = r.zrange("retry_queue", 0, -1)
    if not retry_phones:
        print("没有需要重试的账号")
        return

    # 随机打乱
    random.shuffle(retry_phones)

    # 切分成批次
    batches = []
    i = 0
    while i < len(retry_phones):
        size = random.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)
        batch = retry_phones[i:i + size]
        batches.append(batch)
        i += size

    # 获取重试时间窗口
    start, end = get_retry_window()
    total_minutes = int((end - start).total_seconds() / 60)
    interval = max(total_minutes // len(batches), 5)  # 至少5分钟间隔

    # 生成时间点并写入 schedule
    pipe = r.pipeline()
    for idx, batch in enumerate(batches):
        slot_start = start + timedelta(minutes=idx * interval)
        slot_end = start + timedelta(minutes=(idx + 1) * interval)
        random_minutes = random.randint(0, max(int((slot_end - slot_start).total_seconds() / 60), 1))
        execute_time = slot_start + timedelta(minutes=random_minutes)

        batch_data = json.dumps({
            "phones": batch,
            "batch_no": idx + 1,
            "is_retry": True,
            "created": now_cst().isoformat(),
        })
        pipe.zadd("schedule", {batch_data: execute_time.timestamp()})

        print(f"重试批次 {idx + 1}: {len(batch)} 个账号, 执行时间 {execute_time.strftime('%H:%M')}")

    # 清空 retry_queue
    pipe.delete("retry_queue")
    pipe.execute()
    print(f"\n重试调度表生成完成: {len(batches)} 个批次")


def check_new_accounts():
    """检查新增账号，追加到调度表"""
    r = get_redis()
    config = load_config()
    all_accounts = set(config["accounts"])

    # 获取已在调度表和已完成/放弃的账号
    scheduled_phones = set()
    for item in r.zrange("schedule", 0, -1):
        data = json.loads(item)
        scheduled_phones.update(data["phones"])

    done_phones = r.smembers("done")
    giveup_phones = r.smembers("giveup")
    retry_phones = set(r.zrange("retry_queue", 0, -1))

    known_phones = scheduled_phones | done_phones | giveup_phones | retry_phones
    new_phones = list(all_accounts - known_phones)

    if new_phones:
        print(f"发现 {len(new_phones)} 个新账号: {', '.join(new_phones)}")

        # 计算剩余时间窗口
        now = now_cst()
        end = now.replace(hour=DAY_END_HOUR, minute=DAY_END_MINUTE, second=0, microsecond=0)
        if now >= end:
            # 如果已过正常窗口，放到重试窗口
            end = (now + timedelta(days=1)).replace(hour=RETRY_END_HOUR, minute=0, second=0, microsecond=0)

        remaining_minutes = max(int((end - now).total_seconds() / 60), 10)

        # 切分新账号
        random.shuffle(new_phones)
        batches = []
        i = 0
        while i < len(new_phones):
            size = random.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)
            batch = new_phones[i:i + size]
            batches.append(batch)
            i += size

        interval = remaining_minutes // len(batches)

        # 追加到 schedule
        pipe = r.pipeline()
        for idx, batch in enumerate(batches):
            slot_start = now + timedelta(minutes=idx * interval)
            slot_end = now + timedelta(minutes=(idx + 1) * interval)
            random_minutes = random.randint(0, max(int((slot_end - slot_start).total_seconds() / 60), 1))
            execute_time = slot_start + timedelta(minutes=random_minutes)

            batch_data = json.dumps({
                "phones": batch,
                "batch_no": idx + 1,
                "is_new": True,
                "created": now_cst().isoformat(),
            })
            pipe.zadd("schedule", {batch_data: execute_time.timestamp()})

            print(f"新账号批次 {idx + 1}: {len(batch)} 个账号, 执行时间 {execute_time.strftime('%H:%M')}")

        pipe.execute()


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

def run_batch(batch_phones, user_agents, proxies):
    """并发执行一批账号"""
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


# ====== 执行批次 ======

def execute_batch(batch_data):
    """执行一个批次"""
    r = get_redis()
    config = load_config()
    user_agents = config.get("user_agents", ["Mozilla/5.0"])

    data = json.loads(batch_data)
    phones = data["phones"]
    batch_no = data.get("batch_no", 0)
    is_retry = data.get("is_retry", False)

    prefix = "重试批次" if is_retry else "批次"
    print(f"\n{'='*50}")
    print(f"执行{prefix} {batch_no}: {len(phones)} 个账号")
    print(f"{'='*50}")

    # 获取代理
    proxies = get_proxies(len(phones))
    if len(proxies) < len(phones):
        print(f"代理不足: 需要 {len(phones)} 个，只获取到 {len(proxies)} 个")
        proxies.extend([None] * (len(phones) - len(proxies)))

    # 执行
    results = run_batch(phones, user_agents, proxies)

    # 处理结果
    success_count = 0
    fail_count = 0
    all_reports = []

    for phone in phones:
        report, success = results.get(phone, (f"[{phone}] 无结果", False))

        if success:
            all_reports.append(report)
            r.rpush("reports", report)
            r.sadd("done", phone)
            success_count += 1
        else:
            fail_count += 1
            print(f"[{phone}] 错误详情: {report}")

            # 失败的加入重试队列
            retry_count = r.hincrby("retry_count", phone, 1)
            if retry_count >= MAX_RETRY:
                r.sadd("giveup", phone)
                print(f"[{phone}] 已失败{retry_count}次，放弃")
            else:
                r.zadd("retry_queue", {phone: retry_count})
                print(f"[{phone}] 已失败{retry_count}次，加入重试队列")

    # 批次汇总
    print(f"\n{prefix} {batch_no} 完成: 成功 {success_count} | 失败 {fail_count}")

    return all_reports


# ====== 邮件发送 ======

def send_email(subject, body, email_config):
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = email_config["sender"]
    msg["To"] = email_config["receiver"]

    with smtplib.SMTP_SSL(email_config["smtp_server"], email_config["smtp_port"]) as server:
        server.login(email_config["sender"], email_config["auth_code"])
        server.sendmail(email_config["sender"], email_config["receiver"], msg.as_string())


def send_daily_report():
    """发送日报"""
    r = get_redis()
    config = load_config()
    email_config = config["email"]
    total = len(config["accounts"])

    today = today_str()
    done_count = r.scard("done")
    giveup_count = r.scard("giveup")

    # 获取所有报告
    reports = r.lrange("reports", 0, -1)
    email_body = "\n\n".join(reports)

    subject = f"抽奖日报 {today} (完成{done_count}/{total})"
    if giveup_count:
        subject += f" [放弃{giveup_count}]"

    try:
        send_email(subject, email_body, email_config)
        print(f"\n邮件已发送至 {email_config['receiver']}")
    except Exception as e:
        print(f"\n邮件发送失败: {e}")


# ====== 心跳循环 ======

def tick_loop():
    """主心跳循环"""
    r = get_redis()
    last_check_date = None
    last_new_account_check = 0

    print(f"[{now_cst().strftime('%Y-%m-%d %H:%M:%S')}] 心跳循环启动，间隔 {TICK_INTERVAL} 秒")

    while True:
        now = now_cst()
        current_date = today_str()

        # 检查是否需要生成新的调度表（新的一天）
        if current_date != last_check_date:
            if not is_in_sleep_window():
                print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 新的一天，生成调度表")
                generate_schedule()
                last_check_date = current_date

        # 检查是否需要生成重试调度表（23:30）
        if now.hour == 23 and now.minute >= 30:
            retry_queue_size = r.zcard("retry_queue")
            schedule_size = r.zcard("schedule")
            if retry_queue_size > 0 and schedule_size == 0:
                print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 生成重试调度表")
                generate_retry_schedule()

        # 检查新增账号（每5分钟）
        if time.time() - last_new_account_check > 300:
            check_new_accounts()
            last_new_account_check = time.time()

        # 检查是否有待执行的批次
        if not is_in_sleep_window():
            current_timestamp = now.timestamp()
            tasks = r.zrangebyscore("schedule", "-inf", current_timestamp)

            if tasks:
                for task in tasks:
                    try:
                        execute_batch(task)
                        r.zrem("schedule", task)
                    except Exception as e:
                        print(f"执行批次失败: {e}")
                        r.zrem("schedule", task)

        # 检查是否所有任务完成
        pending = r.zcard("pending")
        retry = r.zcard("retry_queue")
        schedule = r.zcard("schedule")

        if pending == 0 and retry == 0 and schedule == 0 and last_check_date == current_date:
            done = r.scard("done")
            if done > 0:
                print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 今日任务全部完成")
                send_daily_report()
                # 等待到明天
                tomorrow = (now + timedelta(days=1)).replace(hour=DAY_START_HOUR, minute=0, second=0, microsecond=0)
                wait_seconds = (tomorrow - now).total_seconds()
                print(f"下次执行: {tomorrow.strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(max(wait_seconds, 60))
                continue

        time.sleep(TICK_INTERVAL)


# ====== 主入口 ======

def main():
    if "--once" in sys.argv:
        # 立即生成调度表并执行所有批次
        generate_schedule()
        r = get_redis()
        while r.zcard("schedule") > 0:
            tasks = r.zrangebyscore("schedule", "-inf", now_cst().timestamp())
            for task in tasks:
                execute_batch(task)
                r.zrem("schedule", task)
            if r.zcard("schedule") > 0:
                time.sleep(10)
        send_daily_report()
        return

    if "--status" in sys.argv:
        status = get_queue_status()
        print(f"待处理: {status['pending']} | 已完成: {status['done']} | 已放弃: {status['giveup']} | 重试: {status['retry']} | 调度中: {status['schedule']}")

        r = get_redis()
        # 显示调度表
        schedule = r.zrange("schedule", 0, -1, withscores=True)
        if schedule:
            print("\n调度表:")
            for item, timestamp in schedule:
                data = json.loads(item)
                execute_time = datetime.fromtimestamp(timestamp, tz=CST)
                phones = data["phones"]
                prefix = "重试" if data.get("is_retry") else ""
                print(f"  {execute_time.strftime('%H:%M')} - {prefix}批次{data['batch_no']}: {len(phones)} 个账号")
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

        # 添加到 pending
        r = get_redis()
        for phone in phones:
            r.zadd("pending", {phone: 0})

        # 如果今天已有调度表，检查是否需要追加
        if r.zcard("schedule") > 0:
            check_new_accounts()

        print(f"已添加 {len(phones)} 个账号: {', '.join(phones)}")
        return

    if "--generate" in sys.argv:
        generate_schedule()
        return

    if "--retry" in sys.argv:
        generate_retry_schedule()
        return

    # 默认：心跳循环
    tick_loop()


if __name__ == "__main__":
    main()
