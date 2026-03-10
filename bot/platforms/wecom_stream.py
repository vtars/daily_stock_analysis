# -*- coding: utf-8 -*-
"""
===================================
企业微信 Stream 模式适配器
===================================

使用企业微信官方 WebSocket 长连接协议接入智能机器人，
无需公网 IP 和 Webhook 配置。

优势：
- 不需要公网 IP 或域名
- 不需要配置 Webhook URL
- 通过 WebSocket 长连接接收消息
- 更简单的接入方式
- 支持双向通信（被动回复 + 主动推送）

协议文档：
https://developer.work.weixin.qq.com/document/path/101463

官方 Node.js SDK（本模块为 Python 实现）：
https://www.npmjs.com/package/@wecom/aibot-node-sdk

WebSocket 地址: wss://openws.work.weixin.qq.com

连接流程：
1. 发起 WebSocket 连接到 wss://openws.work.weixin.qq.com
2. WebSocket 握手成功
3. 发送订阅请求 (aibot_subscribe) 携带 BotID 和 Secret
4. 企业微信校验凭证
5. 返回订阅结果
6. 连接建立完成，开始接收回调

依赖：
pip install websockets
"""

import json
import logging
import threading
import asyncio
import uuid
import time
from datetime import datetime
from typing import Optional, Callable, Dict, Any, List

logger = logging.getLogger(__name__)

# WebSocket 连接地址
WECOM_WS_URL = "wss://openws.work.weixin.qq.com"

# 尝试导入 websockets
try:
    import websockets
    import websockets.client

    WECOM_STREAM_AVAILABLE = True
except ImportError:
    WECOM_STREAM_AVAILABLE = False
    logger.warning("[WeCom Stream] websockets 未安装，Stream 模式不可用")
    logger.warning("[WeCom Stream] 请运行: pip install websockets")

from bot.models import BotMessage, BotResponse, ChatType


# ═════════════════════════════════════════
# 协议命令常量
# ═════════════════════════════════════════

class WeComCmd:
    """企业微信长连接协议命令"""
    # 客户端 → 服务端
    SUBSCRIBE = "aibot_subscribe"                  # 订阅请求（认证）
    RESPOND_WELCOME = "aibot_respond_welcome_msg"  # 回复欢迎语
    RESPOND_MSG = "aibot_respond_msg"              # 回复消息（被动，需 req_id）
    SEND_MSG = "aibot_send_msg"                    # 主动推送消息（无需 req_id）
    HEARTBEAT = "aibot_heartbeat"                  # 心跳

    # 服务端 → 客户端
    MSG_CALLBACK = "aibot_msg_callback"            # 消息回调
    EVENT_CALLBACK = "aibot_event_callback"        # 事件回调


class WeComEventType:
    """企业微信事件类型"""
    ENTER_CHAT = "enter_chat"                  # 进入会话
    TEMPLATE_CARD = "template_card_event"       # 模板卡片事件
    FEEDBACK = "feedback_event"                 # 用户反馈事件
    DISCONNECTED = "disconnected_event"         # 连接断开事件


# ═════════════════════════════════════════
# 消息处理器
# ═════════════════════════════════════════

class WeComStreamHandler:
    """
    企业微信 Stream 模式消息处理器

    将长连接的消息回调转换为统一的 BotMessage 格式，
    并调用命令分发器处理。
    """

    def __init__(self, on_message: Callable[[BotMessage], BotResponse]):
        """
        Args:
            on_message: 消息处理回调函数，接收 BotMessage 返回 BotResponse
        """
        self._on_message = on_message
        self._logger = logger

    @staticmethod
    def _truncate_log_content(text: str, max_len: int = 200) -> str:
        """截断日志内容"""
        cleaned = text.replace("\n", " ").strip()
        if len(cleaned) > max_len:
            return f"{cleaned[:max_len]}..."
        return cleaned

    def _log_incoming_message(self, message: BotMessage) -> None:
        """记录收到的消息日志"""
        content = message.raw_content or message.content or ""
        summary = self._truncate_log_content(content)
        self._logger.info(
            "[WeCom Stream] Incoming message: msg_id=%s user_id=%s "
            "chat_id=%s chat_type=%s content=%s",
            message.message_id,
            message.user_id,
            message.chat_id,
            getattr(message.chat_type, "value", message.chat_type),
            summary,
        )

    def handle_message(self, data: dict) -> Optional[BotResponse]:
        """
        处理接收到的消息回调 (aibot_msg_callback)

        Args:
            data: 完整的 WebSocket 消息数据

        Returns:
            BotResponse 或 None
        """
        try:
            bot_message = self._parse_msg_callback(data)
            if bot_message is None:
                return None

            self._log_incoming_message(bot_message)

            # 调用消息处理回调
            response = self._on_message(bot_message)
            return response

        except Exception as e:
            self._logger.error(f"[WeCom Stream] 处理消息失败: {e}")
            self._logger.exception(e)
            return None

    def handle_event(self, data: dict) -> Optional[BotResponse]:
        """
        处理接收到的事件回调 (aibot_event_callback)

        Args:
            data: 完整的 WebSocket 消息数据

        Returns:
            BotResponse 或 None（事件回调通常无需回复）
        """
        body = data.get("body", {})
        event_type = body.get("event", {}).get("eventtype", "")
        user_id = body.get("from", {}).get("userid", "")

        self._logger.info(
            "[WeCom Stream] Event: type=%s user_id=%s aibotid=%s",
            event_type, user_id, body.get("aibotid", ""),
        )

        if event_type == WeComEventType.ENTER_CHAT:
            # 用户首次进入会话，可以发送欢迎语
            self._logger.info("[WeCom Stream] 用户 %s 进入会话", user_id)

        elif event_type == WeComEventType.DISCONNECTED:
            # 连接被踢掉（新连接建立时旧连接断开）
            self._logger.warning("[WeCom Stream] 收到连接断开事件，可能有新连接取代了当前连接")

        elif event_type == WeComEventType.FEEDBACK:
            self._logger.info("[WeCom Stream] 用户 %s 发送了反馈", user_id)

        return None

    def _parse_msg_callback(self, data: dict) -> Optional[BotMessage]:
        """
        解析消息回调为统一格式

        消息格式示例 (文本消息):
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "REQUEST_ID"},
            "body": {
                "msgid": "MSGID",
                "aibotid": "AIBOTID",
                "chatid": "CHATID",
                "chattype": "group",
                "from": {"userid": "USERID"},
                "msgtype": "text",
                "text": {"content": "@RobotA hello robot"}
            }
        }
        """
        try:
            body = data.get("body", {})
            headers = data.get("headers", {})

            msg_id = body.get("msgid", "")
            aibot_id = body.get("aibotid", "")
            chat_id = body.get("chatid", "")
            chat_type_str = body.get("chattype", "")
            user_id = body.get("from", {}).get("userid", "")
            msg_type = body.get("msgtype", "")

            # 目前只处理文本消息
            if msg_type == "text":
                raw_content = body.get("text", {}).get("content", "")
            elif msg_type == "mixed":
                # 图文混排：尝试提取文本部分
                raw_content = body.get("mixed", {}).get("content", "")
                if isinstance(raw_content, list):
                    texts = [item.get("text", {}).get("content", "")
                             for item in raw_content if item.get("msgtype") == "text"]
                    raw_content = " ".join(texts)
            elif msg_type == "voice":
                raw_content = "[语音消息]"
            elif msg_type == "image":
                raw_content = "[图片消息]"
            elif msg_type == "file":
                raw_content = "[文件消息]"
            else:
                self._logger.debug(f"[WeCom Stream] 忽略消息类型: {msg_type}")
                return None

            # 提取命令（去除 @机器人）
            content = self._extract_command(raw_content)

            # 会话类型
            if chat_type_str == "group":
                chat_type = ChatType.GROUP
            elif chat_type_str == "single":
                chat_type = ChatType.PRIVATE
            else:
                chat_type = ChatType.UNKNOWN

            # 群聊中 @机器人
            mentioned = "@" in raw_content or chat_type == ChatType.PRIVATE

            return BotMessage(
                platform="wecom",
                message_id=msg_id,
                user_id=user_id,
                user_name=user_id,  # 企微长连接不直接返回用户名
                chat_id=chat_id,
                chat_type=chat_type,
                content=content,
                raw_content=raw_content,
                mentioned=mentioned,
                mentions=[],
                timestamp=datetime.now(),
                raw_data={
                    "req_id": headers.get("req_id", ""),
                    "aibotid": aibot_id,
                    **body,
                },
            )

        except Exception as e:
            self._logger.error(f"[WeCom Stream] 解析消息失败: {e}")
            return None

    def _extract_command(self, text: str) -> str:
        """
        提取命令内容（去除 @机器人）

        企微 @机器人的格式: "@RobotName 命令内容"
        """
        import re
        # 去除 @前缀（非空白字符）
        text = re.sub(r'^@[\S]+\s*', '', text.strip())
        return text.strip()


# ═════════════════════════════════════════
# WebSocket 客户端
# ═════════════════════════════════════════

class WeComStreamClient:
    """
    企业微信 Stream 模式客户端

    封装 WebSocket 长连接协议，提供简单的启动接口。
    协议完全基于企业微信官方文档实现，无需官方 SDK。

    使用方式：
        client = WeComStreamClient(bot_id="xxx", secret="xxx")
        client.start()  # 阻塞运行

        # 或者在后台运行
        client.start_background()
    """

    def __init__(
            self,
            bot_id: Optional[str] = None,
            secret: Optional[str] = None,
            heartbeat_interval: int = 30,
            chatids: Optional[List[str]] = None,
    ):
        """
        Args:
            bot_id: 智能机器人 BotID（不传则从配置读取）
            secret: 长连接专用 Secret（不传则从配置读取）
            heartbeat_interval: 心跳间隔（秒），默认 30 秒
            chatids: 默认推送目标会话列表（单聊填 userid，群聊填 chatid）
        """
        if not WECOM_STREAM_AVAILABLE:
            raise ImportError(
                "websockets 未安装。\n"
                "请运行: pip install websockets"
            )

        self._bot_id = bot_id
        self._secret = secret
        self._heartbeat_interval = heartbeat_interval
        self._chatids: List[str] = list(chatids) if chatids else []

        # 如果未传参，尝试从 DSA 配置或环境变量读取
        if not self._bot_id or not self._secret:
            self._load_credentials()

        if not self._bot_id or not self._secret:
            raise ValueError(
                "企微 Stream 模式需要配置 WECOM_BOT_ID 和 WECOM_BOT_SECRET\n"
                "请在 secrets.yaml → notification 中添加 wecom_bot_id 和 wecom_bot_secret"
            )

        self._ws: Optional[Any] = None
        self._handler: Optional[WeComStreamHandler] = None
        self._background_thread: Optional[threading.Thread] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None  # 后台事件循环引用
        self._running = False
        self._connected = False

    def _load_credentials(self) -> None:
        """从配置或环境变量加载凭证"""
        # 优先从环境变量读取
        self._bot_id = self._bot_id or os.environ.get("WECOM_BOT_ID", "")
        self._secret = self._secret or os.environ.get("WECOM_BOT_SECRET", "")

        # 尝试从 DSA 配置读取
        if not self._bot_id or not self._secret:
            try:
                from src.config import get_config
                config = get_config()
                self._bot_id = self._bot_id or getattr(config, 'wecom_bot_id', '')
                self._secret = self._secret or getattr(config, 'wecom_bot_secret', '')
            except Exception:
                pass

    def _create_message_handler(self) -> Callable[[BotMessage], BotResponse]:
        """创建消息处理函数"""
        def handle_message(message: BotMessage) -> BotResponse:
            from bot.dispatcher import get_dispatcher
            dispatcher = get_dispatcher()
            return dispatcher.dispatch(message)

        return handle_message

    # ─── 协议消息构建 ─────────────────────

    @staticmethod
    def _make_req_id() -> str:
        """生成唯一请求 ID"""
        return str(uuid.uuid4())

    def _build_subscribe(self) -> str:
        """
        构建订阅请求

        {
            "cmd": "aibot_subscribe",
            "headers": {"req_id": "REQUEST_ID"},
            "body": {"bot_id": "BOTID", "secret": "SECRET"}
        }
        """
        return json.dumps({
            "cmd": WeComCmd.SUBSCRIBE,
            "headers": {"req_id": self._make_req_id()},
            "body": {
                "bot_id": self._bot_id,
                "secret": self._secret,
            },
        })

    def _build_reply(self, req_id: str, content: str, stream_id: Optional[str] = None, finish: bool = True) -> str:
        """
        构建回复消息（使用 stream 流式消息格式）

        企微长连接 aibot_respond_msg **仅支持** msgtype=stream，
        不支持 text/markdown（会返回 40008 invalid message type）。

        流式消息机制：
        - 首次使用某个 stream.id 会创建一条新消息
        - 使用相同 stream.id 会更新该消息内容
        - finish=true 表示结束流式消息（一次性回复直接 finish=true 即可）
        - 从首次发送开始，需在 6 分钟内完成

        {
            "cmd": "aibot_respond_msg",
            "headers": {"req_id": "REQUEST_ID"},
            "body": {
                "msgtype": "stream",
                "stream": {
                    "id": "STREAMID",
                    "finish": true,
                    "content": "回复内容"
                }
            }
        }
        """
        if stream_id is None:
            stream_id = str(uuid.uuid4())

        return json.dumps({
            "cmd": WeComCmd.RESPOND_MSG,
            "headers": {"req_id": req_id},
            "body": {
                "msgtype": "stream",
                "stream": {
                    "id": stream_id,
                    "finish": finish,
                    "content": content,
                },
            },
        })

    def _build_welcome(self, req_id: str, content: str) -> str:
        """
        构建欢迎语回复

        {
            "cmd": "aibot_respond_welcome_msg",
            "headers": {"req_id": "REQUEST_ID"},
            "body": {
                "msgtype": "text",
                "text": {"content": "您好！我是智能助手"}
            }
        }
        """
        return json.dumps({
            "cmd": WeComCmd.RESPOND_WELCOME,
            "headers": {"req_id": req_id},
            "body": {
                "msgtype": "text",
                "text": {"content": content},
            },
        })

    def _build_heartbeat(self) -> str:
        """构建心跳包"""
        return json.dumps({
            "cmd": WeComCmd.HEARTBEAT,
            "headers": {"req_id": self._make_req_id()},
        })

    # ─── 核心运行逻辑 ─────────────────────

    async def _heartbeat_loop(self) -> None:
        """心跳保活循环"""
        while self._running and self._connected:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                if self._ws and self._connected:
                    await self._ws.send(self._build_heartbeat())
                    logger.debug("[WeCom Stream] 心跳已发送")
            except Exception as e:
                logger.warning(f"[WeCom Stream] 心跳发送失败: {e}")
                break

    async def _handle_incoming(self, raw: str) -> None:
        """处理收到的 WebSocket 消息"""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"[WeCom Stream] 收到非 JSON 消息: {raw[:200]}")
            return

        cmd = data.get("cmd", "")

        if cmd == WeComCmd.MSG_CALLBACK:
            # 消息回调 → 处理并回复（使用 stream 流式消息格式）
            response = self._handler.handle_message(data)
            if response and response.text:
                req_id = data.get("headers", {}).get("req_id", self._make_req_id())
                reply = self._build_reply(req_id, response.text)
                try:
                    await self._ws.send(reply)
                    logger.debug("[WeCom Stream] 回复已发送")
                except Exception as e:
                    logger.error(f"[WeCom Stream] 回复发送失败: {e}")

        elif cmd == WeComCmd.EVENT_CALLBACK:
            # 事件回调
            body = data.get("body", {})
            event_type = body.get("event", {}).get("eventtype", "")

            if event_type == WeComEventType.ENTER_CHAT:
                # 欢迎语
                req_id = data.get("headers", {}).get("req_id", self._make_req_id())
                welcome = self._build_welcome(req_id, "您好！我是 QTrading 量化交易助手，有什么可以帮您的吗？")
                try:
                    await self._ws.send(welcome)
                    logger.info("[WeCom Stream] 欢迎语已发送")
                except Exception as e:
                    logger.error(f"[WeCom Stream] 欢迎语发送失败: {e}")

            self._handler.handle_event(data)

        else:
            # 其他消息（如订阅响应）
            errcode = data.get("errcode")
            errmsg = data.get("errmsg", "")
            if errcode is not None:
                if errcode == 0:
                    logger.info(f"[WeCom Stream] 服务端响应: {errmsg}")
                else:
                    logger.error(f"[WeCom Stream] 服务端错误: code={errcode}, msg={errmsg}")
            else:
                logger.debug(f"[WeCom Stream] 收到未知消息: {raw[:200]}")

    async def _run_async(self) -> None:
        """异步运行主循环"""
        # 创建消息处理器
        self._handler = WeComStreamHandler(self._create_message_handler())

        logger.info("[WeCom Stream] 正在连接 %s ...", WECOM_WS_URL)

        async with websockets.connect(
                WECOM_WS_URL,
                ping_interval=None,    # 禁用客户端 ping，由 aibot_heartbeat 保活
                ping_timeout=None,
                close_timeout=5,
                max_size=2 ** 20,  # 1MB
        ) as ws:
            self._ws = ws
            logger.info("[WeCom Stream] WebSocket 连接已建立")

            # 发送订阅请求
            subscribe_msg = self._build_subscribe()
            await ws.send(subscribe_msg)
            logger.info("[WeCom Stream] 订阅请求已发送，等待认证...")

            # 等待订阅响应
            try:
                sub_response_raw = await asyncio.wait_for(ws.recv(), timeout=10)
                sub_response = json.loads(sub_response_raw)
                errcode = sub_response.get("errcode", -1)
                errmsg = sub_response.get("errmsg", "unknown")

                if errcode != 0:
                    logger.error(
                        "[WeCom Stream] 订阅失败: code=%s, msg=%s",
                        errcode, errmsg
                    )
                    return

                logger.info("[WeCom Stream] ✅ 订阅成功，开始接收消息")
                self._connected = True

            except asyncio.TimeoutError:
                logger.error("[WeCom Stream] 订阅响应超时（10s）")
                return

            # 启动心跳
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            # 消息接收循环
            try:
                async for message in ws:
                    if not self._running:
                        break
                    await self._handle_incoming(message)
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"[WeCom Stream] 连接关闭: {e}")
            finally:
                self._connected = False
                if self._heartbeat_task:
                    self._heartbeat_task.cancel()

    def start(self) -> None:
        """
        启动 Stream 客户端（阻塞）

        此方法会阻塞当前线程，直到客户端停止。
        """
        self._running = True
        logger.info("[WeCom Stream] 正在启动...")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop  # 保存引用，供 push_message_sync 跨线程使用
        try:
            loop.run_until_complete(self._run_async())
        finally:
            self._loop = None
            loop.close()

    def start_background(self) -> None:
        """
        在后台线程启动 Stream 客户端（非阻塞）

        适用于与其他服务（如监控主循环）同时运行的场景。
        """
        if self._background_thread and self._background_thread.is_alive():
            logger.warning("[WeCom Stream] 客户端已在运行")
            return

        self._running = True
        self._background_thread = threading.Thread(
            target=self._run_in_background,
            daemon=True,
            name="WeComStreamClient",
        )
        self._background_thread.start()
        logger.info("[WeCom Stream] 后台客户端已启动")

    def _run_in_background(self) -> None:
        """后台运行（处理异常和自动重连）"""
        reconnect_delay = 5

        while self._running:
            try:
                self.start()
            except Exception as e:
                logger.error(f"[WeCom Stream] 运行异常: {e}")
                if self._running:
                    logger.info(f"[WeCom Stream] {reconnect_delay} 秒后重连...")
                    time.sleep(reconnect_delay)
                    # 指数退避（最大 60 秒）
                    reconnect_delay = min(reconnect_delay * 2, 60)
                else:
                    break
            else:
                # 正常结束，重置退避
                reconnect_delay = 5

    def stop(self) -> None:
        """停止客户端"""
        self._running = False
        self._connected = False
        logger.info("[WeCom Stream] 客户端已停止")

    @property
    def is_running(self) -> bool:
        """是否正在运行"""
        return self._running

    @property
    def is_connected(self) -> bool:
        """是否已连接并认证"""
        return self._connected

    async def send_message(self, content: str, chatid: str = "", msg_type: str = "markdown") -> bool:
        """
        主动推送消息到指定会话（无需用户先发消息）

        使用 aibot_send_msg 命令，支持向单聊用户或群聊主动推送。

        Args:
            content: 消息内容（支持 Markdown 格式）
            chatid: 目标会话 ID（单聊填 userid，群聊填 chatid）
            msg_type: 消息类型 (markdown / template_card)

        Returns:
            是否发送成功
        """
        if not self._ws or not self._connected:
            logger.error("[WeCom Stream] 未连接，无法发送消息")
            return False

        try:
            body: Dict[str, Any] = {
                "chatid": chatid,
                "msgtype": msg_type,
            }
            if msg_type == "markdown":
                body["markdown"] = {"content": content}
            else:
                body["markdown"] = {"content": content}

            msg = json.dumps({
                "cmd": WeComCmd.SEND_MSG,
                "headers": {"req_id": self._make_req_id()},
                "body": body,
            })
            await self._ws.send(msg)
            logger.info(f"[WeCom Stream] 主动推送已发送到 {chatid}")
            return True
        except Exception as e:
            logger.error(f"[WeCom Stream] 主动推送失败: {e}")
            return False

    def push_message_sync(self, content: str, chatid: str, msg_type: str = "markdown") -> bool:
        """
        同步版主动推送（供非 async 场景使用，如监控回调）

        通过 asyncio.run_coroutine_threadsafe 将 coroutine 提交到后台事件循环，
        从而实现跨线程安全调用。

        Args:
            content: 消息内容（支持 Markdown）
            chatid: 目标会话 ID
            msg_type: 消息类型

        Returns:
            是否发送成功
        """
        if not self._ws or not self._connected:
            logger.error("[WeCom Stream] 未连接，无法推送")
            return False

        if not self._loop or self._loop.is_closed():
            logger.error("[WeCom Stream] 事件循环不可用，无法推送")
            return False

        try:
            future = asyncio.run_coroutine_threadsafe(
                self.send_message(content, chatid, msg_type),
                self._loop,
            )
            return future.result(timeout=10)
        except Exception as e:
            logger.error(f"[WeCom Stream] 同步推送失败: {e}")
            return False

    def push_to_chatids(
        self,
        content: str,
        chatids: Optional[List[str]] = None,
        msg_type: str = "markdown",
    ) -> bool:
        """
        向多个 chatid 批量推送消息（同步版）

        Args:
            content: 消息内容（支持 Markdown）
            chatids: 目标会话列表（不传则使用构造时的默认 chatids）
            msg_type: 消息类型

        Returns:
            是否至少有一个推送成功
        """
        targets = chatids if chatids is not None else self._chatids
        if not targets:
            logger.warning("[WeCom Stream] 无推送目标 chatid")
            return False

        success = False
        for cid in targets:
            if self.push_message_sync(content, cid, msg_type):
                success = True
        return success


# ═════════════════════════════════════════
# 全局工厂函数（与钉钉/飞书保持一致的 API）
# ═════════════════════════════════════════

# 需要导入 os（在模块顶部没有导入，因为放在类内部使用）
import os

_stream_client: Optional[WeComStreamClient] = None


def get_wecom_stream_client(
    bot_id: Optional[str] = None,
    secret: Optional[str] = None,
    chatids: Optional[List[str]] = None,
) -> Optional[WeComStreamClient]:
    """
    获取全局 Stream 客户端实例（单例）

    首次调用时创建实例。如果传入了 bot_id/secret/chatids，
    将用于创建客户端；后续调用返回同一实例。

    Args:
        bot_id: 机器人 BotID（不传则从配置/环境变量读取）
        secret: 长连接 Secret（不传则从配置/环境变量读取）
        chatids: 默认推送目标会话列表
    """
    global _stream_client

    if _stream_client is None and WECOM_STREAM_AVAILABLE:
        try:
            _stream_client = WeComStreamClient(
                bot_id=bot_id,
                secret=secret,
                chatids=chatids,
            )
        except (ImportError, ValueError) as e:
            logger.warning(f"[WeCom Stream] 无法创建客户端: {e}")
            return None

    return _stream_client


def start_wecom_stream_background() -> bool:
    """
    在后台启动企微 Stream 客户端

    Returns:
        是否成功启动
    """
    client = get_wecom_stream_client()
    if client:
        client.start_background()
        return True
    return False
