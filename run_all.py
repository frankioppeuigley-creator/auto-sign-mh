import hashlib
import json
import random
import sys
import time
import uuid
import smtplib
import schedule
import requests
from email.mime.text import MIMEText

# ====== 全局配置 ======
BASE_URL = "https://meihao.v3.api.meihaocvs.com/api"
DATA_BASE_URL = "https://meihao.v3.api.meihaocvs.com/data/api"
APP_VERSION = "4053"


def load_config():
    with open("config.json", "r") as f:
        return json.load(f)


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


def post(path, data=None, auth_token=None, base_url=None, device="1", user_agent="Mozilla/5.0"):
    headers = {
        "Content-Type": "application/json",
        "User-Agent": user_agent,
        "Device": device,
        "version": "3.0",
    }
    if auth_token:
        headers["Authorization"] = auth_token
    payload = prepare_payload(data or {})
    resp = requests.post(
        f"{base_url or BASE_URL}{path}",
        headers=headers,
        json=payload,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


# ====== 登录相关 ======

def get_sms_code(phone, app_uuid, user_agent="Mozilla/5.0"):
    data = {
        "phone": phone,
        "nonce": "abc",
        "timestamp": str(int(time.time() * 1000)),
        "__Device": 2,
        "app_uuid": app_uuid,
    }
    return post("/data/getSmsCode", data, device="2", user_agent=user_agent)


def login(phone, sms_code, app_uuid, user_agent="Mozilla/5.0"):
    data = {
        "login_type": 3,
        "phone": phone,
        "sms_code": sms_code,
        "app_uuid": app_uuid,
    }
    return post("/app/login", data, device="1", user_agent=user_agent)


# ====== 业务接口 ======

def get_sys_component_tpl(auth_token, sys_component_tpl_id="744", user_agent="Mozilla/5.0"):
    return post("/sys/getSysComponentTpl", {"is_dev": "0", "sys_component_tpl_id": sys_component_tpl_id}, auth_token=auth_token, user_agent=user_agent)


def get_coupon_ids_from_tpl(auth_token, sys_component_tpl_id="744", user_agent="Mozilla/5.0"):
    tpl = get_sys_component_tpl(auth_token, sys_component_tpl_id, user_agent)
    json_data = tpl.get("data", {}).get("json_data", [])
    for item in json_data:
        if item.get("component") == "couponComponent":
            coupon_list = item.get("configs", {}).get("couponList", [])
            return [c["link"] for c in coupon_list if c.get("link")]
    return []


def list_user_label(auth_token, user_agent="Mozilla/5.0"):
    return post("/labelV1/listUserLabel", {"user_mobile": None}, auth_token=auth_token, base_url=DATA_BASE_URL, user_agent=user_agent)


def get_label_json(auth_token, user_agent="Mozilla/5.0"):
    result = list_user_label(auth_token, user_agent)
    labels = result.get("data", [])
    return [{"label_item_id": item["label_item_id"]} for item in labels if item.get("label_item_id")]


def add_user_coupon(auth_token, coupon_id_list, user_agent="Mozilla/5.0"):
    data = {
        "coupon_id": None,
        "coupon_id_list": coupon_id_list,
        "user_mobile": None,
        "label_json": get_label_json(auth_token, user_agent),
    }
    return post("/app/addUserCoupon", data, auth_token=auth_token, user_agent=user_agent)


def get_sc_frame_user_vip(auth_token, user_agent="Mozilla/5.0"):
    return post("/app/getScFrameUserVip", {}, auth_token=auth_token, user_agent=user_agent)


def list_extra_pay(auth_token, app_uuid, user_agent="Mozilla/5.0"):
    data = {
        "timetmp": str(int(time.time() * 1000)),
        "app_version": APP_VERSION,
        "app_uuid": app_uuid,
    }
    return post("/app/listExtraPay", data, auth_token=auth_token, user_agent=user_agent)


def click_share(auth_token, user_vip_id, sys_component_tpl_id="744", user_agent="Mozilla/5.0"):
    data = {
        "sys_component_tpl_id": sys_component_tpl_id,
        "user_vip_id": user_vip_id,
        "share_type": 1,
        "label_json": get_label_json(auth_token, user_agent),
    }
    return post("/operations/clickShare", data, auth_token=auth_token, user_agent=user_agent)


def list_user_lottery_coupon(auth_token, lottery_type=10, user_agent="Mozilla/5.0"):
    return post("/lottery/listUserLotteryCoupon", {"lottery_type": lottery_type}, auth_token=auth_token, user_agent=user_agent)


def lottery_winner(auth_token, user_coupon_id, lottery_type=10, code=100, text="优惠券", count=3, user_agent="Mozilla/5.0"):
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
    return post("/lottery/winner", data, auth_token=auth_token, user_agent=user_agent)


# ====== 单账号完整流程 ======

def run_account(phone, user_agent="Mozilla/5.0"):
    lines = []

    try:
        # 1. 登录获取 token
        print(f"\n[{phone}] 开始登录...")
        app_uuid = md5(str(uuid.uuid4()))

        sms_resp = get_sms_code(phone, app_uuid, user_agent)
        if sms_resp.get("code") != 200:
            lines.append(f"[{phone}] 获取验证码失败: {sms_resp}")
            return "\n".join(lines)

        sms_code = sms_resp["data"]
        print(f"[{phone}] 验证码: {sms_code}")

        login_resp = login(phone, sms_code, app_uuid, user_agent)
        if login_resp.get("code") != 200:
            lines.append(f"[{phone}] 登录失败: {login_resp}")
            return "\n".join(lines)

        token = login_resp["data"]["token"]
        print(f"[{phone}] 登录成功")

        # 2. 获取用户信息
        vip_info = get_sc_frame_user_vip(token, user_agent)
        vip_data = vip_info.get("data", {})
        user_vip_id = vip_data.get("user_vip_id")
        user_name = vip_data.get("user_name", "")
        uniapp_device_no = vip_data.get("uniapp_device_no", "")

        lines.append(f"账号: {phone} ({user_name})")

        # 3. 领券
        coupon_ids = get_coupon_ids_from_tpl(token, "744", user_agent) + get_coupon_ids_from_tpl(token, "505", user_agent)
        if coupon_ids:
            add_user_coupon(token, coupon_ids, user_agent)
            lines.append(f"领券: {len(coupon_ids)} 张")
        else:
            lines.append("领券: 0 张")

        # 4. 分享领券
        if user_vip_id:
            click_share(token, user_vip_id, user_agent=user_agent)
            lines.append("分享: 已完成")

        # 5. 获取券列表
        coupons = list_user_lottery_coupon(token, user_agent=user_agent)
        coupon_list = coupons.get("data", [])
        lines.append(f"抽奖券: {len(coupon_list)} 张")

        # 6. 逐个抽奖
        if coupon_list:
            for idx, coupon in enumerate(coupon_list):
                cid = coupon["user_coupon_id"]
                try:
                    result = lottery_winner(token, user_coupon_id=format(cid, 'x'), user_agent=user_agent)
                    data = result.get("data")
                    prize = data.get("item", {}).get("item_caption", "谢谢参与") if isinstance(data, dict) and data.get("item") else "谢谢参与"
                    lines.append(f"  券{cid}: {prize}")
                except Exception as e:
                    lines.append(f"  券{cid}: 异常 {e}")
                if idx < len(coupon_list) - 1:
                    time.sleep(2)

        # 7. 查余额
        extra_pay = list_extra_pay(token, uniapp_device_no or app_uuid, user_agent)
        pay_data = extra_pay.get("data", [])
        score = next((item["cur_score"] for item in pay_data if item["pt_name"] == "会员积分"), 0)
        money = next((item["cur_money"] for item in pay_data if item["pt_name"] == "会员零钱"), 0)
        lines.append(f"积分: {score} | 零钱: {money}")

    except Exception as e:
        lines.append(f"异常: {e}")

    return "\n".join(lines)


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

def main():
    config = load_config()
    accounts = config["accounts"]
    email_config = config["email"]
    user_agents = config.get("user_agents", ["Mozilla/5.0"])

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 开始执行，共 {len(accounts)} 个账号")

    reports = []
    for i, phone in enumerate(accounts):
        ua = random.choice(user_agents)
        print(f"\n{'='*50}")
        print(f"处理第 {i+1}/{len(accounts)} 个账号")
        print(f"UA: {ua[:60]}...")
        print(f"{'='*50}")

        report = run_account(phone, ua)
        reports.append(report)
        print(report)
        print("-" * 40)

        # 账号间休息
        if i < len(accounts) - 1:
            print(f"休息 5 秒...")
            time.sleep(5)

    # 汇总邮件
    today = time.strftime("%Y-%m-%d")
    email_body = "\n\n".join(reports)
    subject = f"抽奖日报 {today}"

    try:
        send_email(subject, email_body, email_config)
        print(f"\n邮件已发送至 {email_config['receiver']}")
    except Exception as e:
        print(f"\n邮件发送失败: {e}")


if __name__ == "__main__":
    if "--once" in sys.argv:
        main()
    else:
        # now = time.localtime()
        # if now.tm_hour > 0 or (now.tm_hour == 0 and now.tm_min >= 1):
        #     print("已过 00:01，立即执行一次")
        #     main()

        schedule.every().day.at("00:01").do(main)
        print("定时任务已启动，每天 00:01 执行，Ctrl+C 退出")
        while True:
            schedule.run_pending()
            time.sleep(30)
