import asyncio
import math
import random
import secrets
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register

from .db import Database

try:
    import botpy.message as botpy_message
    from botpy.types.message import MarkdownPayload
except ImportError:  # pragma: no cover - only QQOfficial runtime needs botpy
    botpy_message = None
    MarkdownPayload = None


QQOFFICIAL_PLATFORMS = {"qq_official", "qq_official_webhook"}
QQOFFICIAL_INTERACTION_INTENT = 1 << 26
PVP_JOIN_BUTTON_ID = "battledick_pvp_join"
PVP_JOIN_BUTTON_DATA_PREFIX = "battledick:pvp_join:"


@dataclass(frozen=True)
class QQOfficialInteractionContext:
    interaction_id: str
    scene: str | None
    chat_type: int | None
    user_openid: str | None = None
    group_openid: str | None = None
    group_member_openid: str | None = None
    button_id: str | None = None
    button_data: str | None = None
    message_id: str | None = None
    user_name: str | None = None


class QQOfficialEventPlatformProxy:
    def __init__(self, client: Any, platform_name: str | None):
        self.client = client
        self._platform_name = platform_name

    def meta(self) -> Any:
        return SimpleNamespace(name=self._platform_name)


def _first_non_empty_str(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value)
        if text:
            return text
    return None


def _get_field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _markdown_payload(content: str) -> Any:
    if MarkdownPayload is None:
        return {"content": content}
    return MarkdownPayload(content=content)


def _md_inline(value: Any) -> str:
    text = str(value)
    return (
        text.replace("\\", "\\\\")
        .replace("`", "\\`")
        .replace("*", "\\*")
        .replace("_", "\\_")
        .replace("[", "\\[")
        .replace("]", "\\]")
    )


def _extract_message_reference_id(raw_message: Any, message_obj: Any) -> str | None:
    return _first_non_empty_str(
        getattr(raw_message, "id", None),
        getattr(message_obj, "message_id", None),
    )


def _add_passive_reply_context(
    payload: dict[str, Any],
    *,
    msg_id: str | None = None,
    event_id: str | None = None,
    msg_seq: int | None = None,
) -> dict[str, Any]:
    if msg_id:
        payload["msg_id"] = msg_id
    elif event_id:
        payload["event_id"] = event_id
    if payload.get("msg_id") or payload.get("event_id"):
        payload["msg_seq"] = msg_seq if msg_seq is not None else random.randint(1, 10000)
    return payload


def _build_pvp_join_keyboard(gid: str) -> dict[str, Any]:
    return {
        "content": {
            "rows": [
                {
                    "buttons": [
                        {
                            "id": PVP_JOIN_BUTTON_ID,
                            "render_data": {
                                "label": "应战",
                                "visited_label": "已应战",
                                "style": 1,
                            },
                            "action": {
                                "type": 1,
                                "permission": {
                                    "type": 2,
                                    "specify_user_ids": [],
                                    "specify_role_ids": [],
                                },
                                "click_limit": 0,
                                "data": f"{PVP_JOIN_BUTTON_DATA_PREFIX}{gid}",
                                "at_bot_show_channel_list": False,
                                "unsupport_tips": "当前客户端不支持该按钮，请发送 /comeon 应战",
                            },
                        }
                    ]
                }
            ]
        }
    }


def _build_qqofficial_button_payload(content: str, gid: str) -> dict[str, Any]:
    return {
        "msg_type": 2,
        "markdown": _markdown_payload(content),
        "keyboard": _build_pvp_join_keyboard(gid),
    }


def _build_qqofficial_text_payload(content: str) -> dict[str, Any]:
    return {
        "msg_type": 2,
        "markdown": _markdown_payload(content),
        "keyboard": None,
    }


def _extract_interaction_user_name(interaction: Any) -> str | None:
    data = _get_field(interaction, "data")
    resolved = _get_field(data, "resolved")
    holders = []
    for source in (interaction, resolved, data):
        if source is None:
            continue
        for holder_name in ("member", "user", "author"):
            holder = _get_field(source, holder_name)
            if holder is not None:
                holders.append(holder)

    for holder in holders:
        if holder is None:
            continue
        name = _first_non_empty_str(
            _get_field(holder, "nick"),
            _get_field(holder, "nickname"),
            _get_field(holder, "username"),
            _get_field(holder, "global_name"),
            _get_field(holder, "name"),
            _get_field(holder, "display_name"),
        )
        if name:
            return name
    return None


def _extract_interaction_context(interaction: Any) -> QQOfficialInteractionContext | None:
    if interaction is None:
        return None
    data = _get_field(interaction, "data")
    resolved = _get_field(data, "resolved")
    button_id = _get_field(resolved, "button_id")
    button_data = _get_field(resolved, "button_data")
    message_id = _get_field(resolved, "message_id")
    return QQOfficialInteractionContext(
        interaction_id=str(_get_field(interaction, "id", "") or ""),
        scene=_get_field(interaction, "scene"),
        chat_type=_get_field(interaction, "chat_type"),
        user_openid=_get_field(interaction, "user_openid"),
        group_openid=_get_field(interaction, "group_openid"),
        group_member_openid=_get_field(interaction, "group_member_openid"),
        button_id=str(button_id) if button_id is not None else None,
        button_data=str(button_data) if button_data is not None else None,
        message_id=str(message_id) if message_id is not None else None,
        user_name=_extract_interaction_user_name(interaction),
    )


def _is_qqofficial_event(event: AstrMessageEvent) -> bool:
    platform_name = event.get_platform_name() if hasattr(event, "get_platform_name") else None
    if platform_name in QQOFFICIAL_PLATFORMS:
        return True
    event_type = type(event)
    module_name = event_type.__module__.lower()
    return (
        event_type.__name__ in {"QQOfficialMessageEvent", "QQOfficialWebhookMessageEvent"}
        and module_name.startswith(
            (
                "astrbot.core.platform.sources.qqofficial.",
                "astrbot.core.platform.sources.qqofficial_webhook.",
            )
        )
    )


def _is_group_raw_message(raw_message: Any) -> bool:
    if raw_message is None:
        return False
    if botpy_message is not None and isinstance(raw_message, botpy_message.GroupMessage):
        return True
    return bool(getattr(raw_message, "group_openid", None))


def _extract_pvp_join_gid(context: QQOfficialInteractionContext) -> str | None:
    data = context.button_data or ""
    if data.startswith(PVP_JOIN_BUTTON_DATA_PREFIX):
        return data[len(PVP_JOIN_BUTTON_DATA_PREFIX) :]
    return None


@register("battledick", "letr", "斗鸡插件", "0.1.0")
class MyPlugin(Star):
    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self.config = config or {}
        self._load_settings()
        self._rng = secrets.SystemRandom()
        self.active_challenges = {}  # {group_id: {"data": dict, "task": Task}}
        self._interaction_hook_installed = False

    def _get_config_value(self, *keys, default):
        value = self.config
        for key in keys:
            if not isinstance(value, dict):
                return default
            value = value.get(key)
            if value is None:
                return default
        return value

    @staticmethod
    def _coerce_float(value, default):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _coerce_int(value, default):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _load_settings(self):
        growth_min = self._coerce_float(
            self._get_config_value("growth", "min_cm", default=0.1),
            0.1,
        )
        growth_max = self._coerce_float(
            self._get_config_value("growth", "max_cm", default=5.0),
            5.0,
        )
        if growth_min <= 0 or growth_max <= 0 or growth_min > growth_max:
            growth_min, growth_max = 0.1, 5.0
        self.growth_min = growth_min
        self.growth_max = growth_max
        self.growth_daily_limit = self._coerce_int(
            self._get_config_value("growth", "daily_limit", default=1),
            1,
        )
        if self.growth_daily_limit <= 0:
            self.growth_daily_limit = 1

        lu_min = self._coerce_float(
            self._get_config_value("lu", "lu_min_cm", default=0.1),
            0.1,
        )
        lu_max = self._coerce_float(
            self._get_config_value("lu", "lu_max_cm", default=1.0),
            1.0,
        )
        if lu_min <= 0 or lu_max <= 0 or lu_min > lu_max:
            lu_min, lu_max = 0.1, 1
        self.lu_min = lu_min
        self.lu_max = lu_max
        self.lu_cp_num = self._coerce_int(
            self._get_config_value("lu", "lu_cp_num", default=3),
            3,
        )
        if self.lu_cp_num < 0:
            self.lu_cp_num = 0
        self.lu_cp_mag = self._coerce_float(
            self._get_config_value("lu", "lu_cp_mag", default=0.1),
            0.1,
        )
        if self.lu_cp_mag < 0:
            self.lu_cp_mag = 0.0
        milk_min = self._coerce_float(
            self._get_config_value("milk", "milk_min_ml", default=5.0),
            5.0,
        )
        milk_max = self._coerce_float(
            self._get_config_value("milk", "milk_max_ml", default=30.0),
            30.0,
        )
        if milk_min <= 0 or milk_max <= 0 or milk_min > milk_max:
            milk_min, milk_max = 5.0, 30.0
        self.milk_min = milk_min
        self.milk_max = milk_max

        self.decay_enable = bool(
            self._get_config_value("decay", "enable", default=False)
        )
        self.decay_grace_days = self._coerce_int(
            self._get_config_value("decay", "grace_days", default=3),
            3,
        )
        if self.decay_grace_days < 0:
            self.decay_grace_days = 0

        self.decay_mode = self._get_config_value("decay", "mode", default="fixed")
        if self.decay_mode not in ("fixed", "ratio"):
            self.decay_mode = "fixed"

        self.decay_fixed_per_day = self._coerce_float(
            self._get_config_value("decay", "fixed_cm_per_day", default=0.5),
            0.5,
        )
        if self.decay_fixed_per_day <= 0:
            self.decay_fixed_per_day = 0.5

        self.decay_ratio_per_day = self._coerce_float(
            self._get_config_value("decay", "ratio_per_day", default=0.05),
            0.05,
        )
        if self.decay_ratio_per_day <= 0 or self.decay_ratio_per_day >= 1:
            self.decay_ratio_per_day = 0.05

        self.pvp_timeout_seconds = self._coerce_int(
            self._get_config_value("pvp", "timeout_seconds", default=60),
            60,
        )
        if self.pvp_timeout_seconds <= 0:
            self.pvp_timeout_seconds = 60

        self.win_prob_power = self._coerce_float(
            self._get_config_value("pvp", "win_power", default=0.7),
            0.7,
        )
        if self.win_prob_power <= 0:
            self.win_prob_power = 0.7

        self.win_prob_min_length = self._coerce_float(
            self._get_config_value("pvp", "min_length_for_probability", default=0.1),
            0.1,
        )
        if self.win_prob_min_length <= 0:
            self.win_prob_min_length = 0.1

        self.odds_enable = bool(
            self._get_config_value("pvp", "odds_enable", default=False)
        )
        self.odds_min = self._coerce_float(
            self._get_config_value("pvp", "odds_min", default=0.6),
            0.6,
        )
        self.odds_max = self._coerce_float(
            self._get_config_value("pvp", "odds_max", default=1.6),
            1.6,
        )
        if self.odds_min <= 0:
            self.odds_min = 0.6
        if self.odds_max <= self.odds_min:
            self.odds_max = self.odds_min

    def _calc_odds(self, win_prob: float) -> float:
        if not self.odds_enable:
            return 1.0
        odds = self.odds_min + (1 - win_prob) * (self.odds_max - self.odds_min)
        odds = max(self.odds_min, min(self.odds_max, odds))
        return round(odds, 2)

    @staticmethod
    def _fmt_len(value: float) -> str:
        return f"{round(float(value), 2):.2f}"

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    def _calc_lu_fatigue_pressure(self, lu_count: int, current_len: float) -> float:
        """Return fatigue pressure used to skew random consume/reward distributions."""
        if self.lu_cp_mag <= 0 or lu_count <= self.lu_cp_num:
            return 0.0

        over_limit = lu_count - self.lu_cp_num
        length_ratio = current_len / (current_len + 25.0)
        length_factor = 0.45 + 1.25 * length_ratio
        pressure = math.pow(over_limit, 1.15) * self.lu_cp_mag * length_factor
        return self._clamp(pressure, 0.0, 5.0)

    def _roll_lu_outcome(
        self,
        current_len: float,
        lu_count: int,
    ) -> tuple[float, float, float]:
        pressure = self._calc_lu_fatigue_pressure(lu_count, current_len)

        consume_min = self.lu_min * (1.0 + pressure * 0.08)
        consume_max = self.lu_max * (1.0 + pressure * 0.35)
        if consume_min > consume_max:
            consume_min = consume_max
        consume_mode_bias = self._clamp(0.5 + pressure * 0.22, 0.5, 0.96)
        consume_mode = consume_min + (consume_max - consume_min) * consume_mode_bias
        lu_length = self._rng.triangular(consume_min, consume_max, consume_mode)
        lu_length = self._clamp(lu_length, 0.0, current_len)

        milk_min = self.milk_min * max(0.15, 1.0 - pressure * 0.10)
        milk_max = self.milk_max * max(0.25, 1.0 - pressure * 0.20)
        if milk_min > milk_max:
            milk_min = milk_max
        milk_mode_bias = self._clamp(0.5 - pressure * 0.20, 0.04, 0.5)
        milk_mode = milk_min + (milk_max - milk_min) * milk_mode_bias
        milk_amount = self._rng.triangular(milk_min, milk_max, milk_mode)

        return round(lu_length, 2), round(milk_amount, 2), pressure

    def _apply_decay(self, user_id: str, user_name: str = "") -> None:
        if not self.decay_enable:
            return
        today = datetime.now().date()
        today_str = today.strftime("%Y-%m-%d")
        last_date_str = self.db.get_last_growth_date(user_id)
        if not last_date_str:
            self.db.set_last_growth_date(user_id, today_str)
            return
        try:
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
        except ValueError:
            self.db.set_last_growth_date(user_id, today_str)
            return
        days_passed = (today - last_date).days
        if days_passed <= self.decay_grace_days:
            return
        decay_days = days_passed - self.decay_grace_days
        if decay_days <= 0:
            return

        current_len = self.db.get_user_length(user_id)
        if current_len <= 0:
            self.db.set_last_growth_date(user_id, today_str)
            return

        if self.decay_mode == "ratio":
            length = current_len
            for _ in range(decay_days):
                length *= 1 - self.decay_ratio_per_day
        else:
            length = current_len - decay_days * self.decay_fixed_per_day

        length = max(0.0, round(length, 2))
        self.db.update_user_length(user_id, user_name, length)
        self.db.set_last_growth_date(user_id, today_str)

    async def initialize(self):
        plugin_data_path = StarTools.get_data_dir(self.name)
        self.db = Database(str(plugin_data_path / "data.db"))
        self._install_interaction_hook()

    @filter.on_platform_loaded()
    async def _on_platform_loaded(self):
        self._install_interaction_hook()

    def get_gid(self, event: AstrMessageEvent):
        """安全获取群组ID标识"""
        try:
            return event.get_group_id() or f"private_{event.get_sender_id()}"
        except Exception:
            return str(event.get_sender_id())

    async def cancel_existing_task(self, gid: str):
        """清理并取消现有的超时任务"""
        if gid in self.active_challenges:
            task = self.active_challenges[gid].get("task")
            if task and not task.done():
                task.cancel()
            del self.active_challenges[gid]

    def _install_interaction_hook(self) -> None:
        platform_manager = getattr(self.context, "platform_manager", None)
        platforms = getattr(platform_manager, "platform_insts", []) or []
        patched = False
        for platform in platforms:
            platform_meta = platform.meta() if hasattr(platform, "meta") else None
            if getattr(platform_meta, "name", None) not in QQOFFICIAL_PLATFORMS:
                continue
            client = getattr(platform, "client", None)
            if client is None:
                continue
            self._ensure_interaction_intent(client)
            if self._patch_qqofficial_client(platform, client):
                patched = True
        if patched:
            self._interaction_hook_installed = True

    def _install_interaction_hook_from_event(self, event: AstrMessageEvent) -> None:
        client = getattr(event, "bot", None)
        if client is None:
            logger.warning("[BattleDick] 当前事件缺少 bot client，无法安装 QQOfficial 按钮回调。")
            return

        self._ensure_interaction_intent(client)
        platform = QQOfficialEventPlatformProxy(
            client,
            event.get_platform_name() if hasattr(event, "get_platform_name") else None,
        )
        if self._patch_qqofficial_client(platform, client):
            self._interaction_hook_installed = True

    def _ensure_interaction_intent(self, client: Any) -> None:
        current_intents = getattr(client, "intents", None)
        if not isinstance(current_intents, int):
            logger.warning(
                "[BattleDick] 无法确认 QQOfficial interaction intent: client=%s, intents=%r",
                type(client).__name__,
                current_intents,
            )
            return
        if current_intents & QQOFFICIAL_INTERACTION_INTENT:
            return
        client.intents = current_intents | QQOFFICIAL_INTERACTION_INTENT
        logger.warning(
            "[BattleDick] 已为 QQOfficial client 补充 interaction intent: %s -> %s。"
            "如果 websocket 已连接，需要重启/重连 QQOfficial 平台后才会收到按钮点击事件。",
            current_intents,
            client.intents,
        )

    def _patch_qqofficial_client(self, platform: Any, client: Any) -> bool:
        if (
            getattr(client, "_battledick_button_hook_installed", False)
            and getattr(client, "_battledick_button_hook_owner", None) == id(self)
        ):
            return False

        plugin = self
        original = getattr(
            client,
            "_battledick_button_hook_original",
            getattr(client, "on_interaction_create", None),
        )

        async def on_interaction_create(interaction: Any):
            await plugin._handle_qqofficial_interaction(platform, interaction)
            if original and original is not on_interaction_create:
                maybe = original(interaction)
                if asyncio.iscoroutine(maybe):
                    await maybe

        client.on_interaction_create = on_interaction_create
        client._battledick_button_hook_installed = True
        client._battledick_button_hook_owner = id(self)
        client._battledick_button_hook_original = original
        logger.info(
            "[BattleDick] 已安装 QQOfficial interaction hook: platform=%r, client=%s",
            platform.meta().name if hasattr(platform, "meta") else None,
            type(client).__name__,
        )
        return True

    async def _handle_qqofficial_interaction(self, platform: Any, interaction: Any) -> None:
        context = _extract_interaction_context(interaction)
        if not context or not context.interaction_id:
            logger.warning("[BattleDick] 忽略按钮回调：缺少 interaction_id。")
            return

        gid = _extract_pvp_join_gid(context)
        if not gid:
            return

        logger.info(
            "[BattleDick] 收到 QQOfficial PVP 按钮回调: interaction_id=%s, message_id=%s, scene=%s, group_openid=%s",
            context.interaction_id,
            context.message_id,
            context.scene,
            context.group_openid,
        )

        try:
            await platform.client.api.on_interaction_result(context.interaction_id, 0)
        except Exception as exc:
            logger.warning(f"[BattleDick] QQOfficial 按钮 ACK 失败: {exc}")

        participant_id = self._resolve_interaction_participant_id(context, gid)
        if not participant_id:
            await self._send_qqofficial_group_text(
                platform,
                context.group_openid,
                self._format_qqofficial_notice("无法应战", "无法识别应战者，无法加入决斗。"),
                msg_id=context.message_id,
            )
            return

        participant_name = (
            context.user_name
            or self.db.get_user_profile_name(participant_id)
            or self.db.get_user_name(participant_id)
            or f"玩家{participant_id[-6:]}"
        )
        logger.info(
            "[BattleDick] QQOfficial PVP 应战者解析: participant_id=%s, participant_name=%s",
            participant_id,
            participant_name,
        )
        result_text = await self._join_pvp(
            gid,
            participant_id,
            participant_name,
            use_markdown=True,
        )
        if result_text is None:
            result_text = self._format_qqofficial_notice("决斗已结束", "当前没有可加入的决斗。")
        await self._send_qqofficial_group_text(
            platform,
            context.group_openid,
            result_text,
            msg_id=context.message_id,
        )

    def _resolve_interaction_participant_id(
        self,
        context: QQOfficialInteractionContext,
        gid: str,
    ) -> str | None:
        candidates = []
        for value in (context.group_member_openid, context.user_openid):
            if value and value not in candidates:
                candidates.append(value)
        if not candidates:
            return None

        challenge = self.active_challenges.get(gid)
        initiator_id = None
        if challenge:
            initiator_id = challenge["data"].get("initiator_id")
        if initiator_id in candidates:
            return initiator_id

        for candidate in candidates:
            if self.db.get_user_length(candidate) > 0:
                return candidate
        return candidates[0]

    async def _send_qqofficial_group_text(
        self,
        platform: Any,
        group_openid: str | None,
        content: str,
        *,
        msg_id: str | None = None,
    ) -> bool:
        if not group_openid:
            logger.warning("[BattleDick] QQOfficial 按钮回复缺少 group_openid。")
            return False
        payload = _build_qqofficial_text_payload(content)
        _add_passive_reply_context(payload, msg_id=msg_id)
        try:
            await platform.client.api.post_group_message(group_openid=group_openid, **payload)
            return True
        except Exception as exc:
            logger.exception(f"[BattleDick] QQOfficial 按钮回复失败: {exc}")
            return False

    async def _send_qqofficial_markdown(
        self,
        event: AstrMessageEvent,
        content: str,
    ) -> bool:
        if not _is_qqofficial_event(event):
            return False

        message_obj = getattr(event, "message_obj", None)
        raw_message = getattr(message_obj, "raw_message", None)
        if not _is_group_raw_message(raw_message):
            logger.warning("[BattleDick] 当前 QQOfficial 原始消息不是群消息，无法发送 Markdown。")
            return False

        group_openid = getattr(raw_message, "group_openid", None)
        if not group_openid:
            logger.warning("[BattleDick] QQOfficial 原始群消息缺少 group_openid，无法发送 Markdown。")
            return False

        payload = _build_qqofficial_text_payload(content)
        _add_passive_reply_context(
            payload,
            msg_id=_extract_message_reference_id(raw_message, message_obj),
            msg_seq=getattr(raw_message, "msg_seq", None),
        )
        try:
            await event.bot.api.post_group_message(group_openid=group_openid, **payload)
            return True
        except Exception as exc:
            logger.exception(f"[BattleDick] QQOfficial Markdown 发送失败: {exc}")
            return False

    async def _reply_group(
        self,
        event: AstrMessageEvent,
        plain_text: str,
        markdown_text: str | None = None,
    ) -> bool:
        if await self._send_qqofficial_markdown(event, markdown_text or plain_text):
            event.stop_event()
            return True
        return False

    async def _send_qqofficial_pvp_button(
        self,
        event: AstrMessageEvent,
        gid: str,
        content: str,
    ) -> bool:
        if not _is_qqofficial_event(event):
            return False
        self._install_interaction_hook_from_event(event)

        message_obj = getattr(event, "message_obj", None)
        raw_message = getattr(message_obj, "raw_message", None)
        if not _is_group_raw_message(raw_message):
            logger.warning("[BattleDick] 当前 QQOfficial 原始消息不是群消息，无法发送 PVP 按钮。")
            return False

        group_openid = getattr(raw_message, "group_openid", None)
        if not group_openid:
            logger.warning("[BattleDick] QQOfficial 原始群消息缺少 group_openid，无法发送 PVP 按钮。")
            return False

        payload = _build_qqofficial_button_payload(content, gid)
        _add_passive_reply_context(
            payload,
            msg_id=_extract_message_reference_id(raw_message, message_obj),
            msg_seq=getattr(raw_message, "msg_seq", None),
        )
        try:
            await event.bot.api.post_group_message(
                group_openid=group_openid,
                **payload,
            )
            return True
        except Exception as exc:
            logger.exception(f"[BattleDick] QQOfficial PVP 按钮发送失败: {exc}")
            return False

    async def _join_pvp(
        self,
        gid: str,
        participant_id: str,
        participant_name: str,
        *,
        use_markdown: bool = False,
    ) -> str | None:
        if gid not in self.active_challenges:
            return None

        challenge_data = self.active_challenges[gid]["data"]
        self._apply_decay(participant_id, participant_name)

        if participant_id == challenge_data["initiator_id"]:
            message = "你不能左右互搏！"
            return self._format_qqofficial_notice("无法应战", message) if use_markdown else message

        participant_len = self.db.get_user_length(participant_id)
        bet = challenge_data["bet"]

        if participant_len < bet:
            message = f"你的长度不足 {bet} cm，去锻炼一下再来吧"
            return self._format_qqofficial_notice("无法应战", message) if use_markdown else message

        await self.cancel_existing_task(gid)

        try:
            initiator_len = challenge_data["initiator_length"]

            pow_a = self.win_prob_power
            initiator_base = math.log1p(max(initiator_len, self.win_prob_min_length))
            participant_base = math.log1p(max(participant_len, self.win_prob_min_length))
            initiator_power = math.pow(initiator_base, pow_a)
            participant_power = math.pow(participant_base, pow_a)
            win_prob = initiator_power / (initiator_power + participant_power)

            is_initiator_win = self._rng.random() < win_prob

            if is_initiator_win:
                win_id, win_name = (
                    challenge_data["initiator_id"],
                    challenge_data["initiator_name"],
                )
                lose_id, lose_name = participant_id, participant_name
            else:
                win_id, win_name = participant_id, participant_name
                lose_id, lose_name = (
                    challenge_data["initiator_id"],
                    challenge_data["initiator_name"],
                )

            winner_prob = win_prob if is_initiator_win else 1 - win_prob
            odds = self._calc_odds(winner_prob)
            effective_bet = round(bet * odds, 2)
            max_loss = round(min(initiator_len, participant_len), 2)
            if effective_bet > max_loss:
                effective_bet = max_loss

            self.db.adjust_user_length(win_id, effective_bet, win_name)
            self.db.adjust_user_length(lose_id, -effective_bet, lose_name)

            res_win = self.db.get_user_length(win_id)
            res_lose = self.db.get_user_length(lose_id)

            if use_markdown:
                return self._format_qqofficial_pvp_result(
                    win_name=win_name,
                    lose_name=lose_name,
                    effective_bet=effective_bet,
                    odds=odds,
                    res_win=res_win,
                    res_lose=res_lose,
                )
            return self._format_plain_pvp_result(
                win_name=win_name,
                lose_name=lose_name,
                effective_bet=effective_bet,
                odds=odds,
                res_win=res_win,
                res_lose=res_lose,
            )
        except Exception as e:
            logger.error(f"PVP Logic Error: {e}")
            message = "计算对局时服务器卡了一下，双方各回各家。"
            return self._format_qqofficial_notice("结算失败", message) if use_markdown else message

    def _format_qqofficial_notice(self, title: str, message: str) -> str:
        return f"### {_md_inline(title)}\n\n> {_md_inline(message)}"

    def _format_qqofficial_growth_result(
        self,
        user_name: str,
        growth_amount: float,
        new_len: float,
    ) -> str:
        return (
            "## ✨ 今日锻炼完成\n\n"
            f"**{_md_inline(user_name)}** 的长度增加了 **{self._fmt_len(growth_amount)} cm**\n\n"
            f"> 当前长度：**{self._fmt_len(new_len)} cm**"
        )

    def _format_qqofficial_length(self, user_name: str, length: float) -> str:
        return (
            "## 📏 长度查询\n\n"
            f"**{_md_inline(user_name)}** 当前长度\n\n"
            f"> **{self._fmt_len(length)} cm**"
        )

    def _format_qqofficial_lu_result(
        self,
        user_name: str,
        new_len: float,
        milk_amount: float,
        lu_count: int,
        pressure: float,
    ) -> str:
        fatigue = ""
        if pressure > 0:
            fatigue = f"\n\n> 疲劳惩罚：第 **{lu_count}** 次，收益下滑、消耗上升"
        return (
            "## 🦌 降落完成\n\n"
            f"机长 **{_md_inline(user_name)}** 已成功降落\n\n"
            f"- 当前长度：**{self._fmt_len(new_len)} cm**\n"
            f"- 产生金液：**{self._fmt_len(milk_amount)} ml**"
            f"{fatigue}"
        )

    def _format_qqofficial_milk(self, user_name: str, milk_ml: float) -> str:
        return (
            "## 🥛 金液查询\n\n"
            f"**{_md_inline(user_name)}** 当前金液\n\n"
            f"> **{self._fmt_len(milk_ml)} ml**"
        )

    def _format_qqofficial_pvp_prompt(
        self,
        initiator_name: str,
        bet: float,
        current_len: float,
    ) -> str:
        return (
            "## ⚔️ 决斗挑战\n\n"
            f"**{_md_inline(initiator_name)}** 发起了 **{self._fmt_len(bet)} cm** 赌斗\n\n"
            f"> 点击下方 **应战** 按钮加入对局\n\n"
            f"- 发起者长度：**{self._fmt_len(current_len)} cm**\n"
            f"- 等待时间：**{self.pvp_timeout_seconds} 秒**\n"
            f"- 备用指令：`/comeon`\n\n"
            "胜负会按双方长度动态计算。"
        )

    def _format_qqofficial_pvp_result(
        self,
        *,
        win_name: str,
        lose_name: str,
        effective_bet: float,
        odds: float,
        res_win: float,
        res_lose: float,
    ) -> str:
        return (
            "## ⚔️ 决斗结束\n\n"
            f"**👑 胜者：{_md_inline(win_name)}**\n\n"
            f"- 收益：**+{self._fmt_len(effective_bet)} cm**\n"
            f"- 当前长度：**{self._fmt_len(res_win)} cm**\n\n"
            f"**💀 败者：{_md_inline(lose_name)}**\n\n"
            f"- 损失：**-{self._fmt_len(effective_bet)} cm**\n"
            f"- 当前长度：**{self._fmt_len(res_lose)} cm**\n\n"
            f"> 赔率：**{odds}x**"
        )

    def _format_plain_pvp_result(
        self,
        *,
        win_name: str,
        lose_name: str,
        effective_bet: float,
        odds: float,
        res_win: float,
        res_lose: float,
    ) -> str:
        return (
            f"⚔️ 决斗结束！\n"
            f"━━━━━━━━━━━━━━\n"
            f"👑 胜者：{win_name} (+{self._fmt_len(effective_bet)}cm)\n"
            f"💀 败者：{lose_name} (-{self._fmt_len(effective_bet)}cm)\n"
            f"🎲 赔率：{odds}x\n"
            f"━━━━━━━━━━━━━━\n"
            f"📊 战报：{win_name}({self._fmt_len(res_win)}cm) | "
            f"{lose_name}({self._fmt_len(res_lose)}cm)"
        )

    def _remember_sender_profile(self, event: AstrMessageEvent) -> None:
        try:
            user_id = event.get_sender_id()
            user_name = event.get_sender_name()
        except Exception as exc:
            logger.debug(f"[BattleDick] 记录用户昵称失败：无法读取发送者信息: {exc}")
            return
        if user_id and user_name:
            self.db.upsert_user_profile(str(user_id), str(user_name))

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def remember_sender_profile(self, event: AstrMessageEvent):
        self._remember_sender_profile(event)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("growth")
    async def growth(self, event: AstrMessageEvent):
        """核心功能：每日成长"""
        uid = event.get_sender_id()
        uname = event.get_sender_name()

        self._apply_decay(uid, uname)
        date_str = datetime.now().strftime("%Y-%m-%d")
        if self.growth_daily_limit > 0:
            used_count = self.db.get_daily_growth_count(uid, date_str)
            if used_count >= self.growth_daily_limit:
                plain_text = f"今天的锻炼已达到上限 ({self.growth_daily_limit} 次)，注意身体哦"
                if await self._reply_group(
                    event,
                    plain_text,
                    self._format_qqofficial_notice(
                        "今日锻炼已完成",
                        f"今天的锻炼已达到上限 ({self.growth_daily_limit} 次)，注意身体哦",
                    ),
                ):
                    return
                yield event.plain_result(
                    plain_text
                )
                return

        # 随机增长量
        growth_amount = round(random.uniform(self.growth_min, self.growth_max), 2)

        try:
            # 这里的 update_user_length 逻辑应包含：若不存在则初始化，若存在则累加
            current_len = self.db.get_user_length(uid)
            new_len = round(current_len + growth_amount, 2)
            self.db.update_user_length(uid, uname, new_len)
            self.db.increment_daily_growth(uid, date_str)
            self.db.set_last_growth_date(uid, date_str)

            plain_text = (
                f"✨ {uname} 进行了晨间锻炼！\n"
                f"长度增加了 {self._fmt_len(growth_amount)} cm，"
                f"当前长度：{self._fmt_len(new_len)} cm。"
            )
            if await self._reply_group(
                event,
                plain_text,
                self._format_qqofficial_growth_result(uname, growth_amount, new_len),
            ):
                return
            yield event.plain_result(plain_text)
        except Exception as e:
            logger.error(f"Growth Error: {e}")
            plain_text = "锻炼时抽筋了，请稍后再试。"
            if await self._reply_group(
                event,
                plain_text,
                self._format_qqofficial_notice("锻炼失败", plain_text),
            ):
                return
            yield event.plain_result(plain_text)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("len")
    async def show_length(self, event: AstrMessageEvent):
        """查询当前长度"""
        uid = event.get_sender_id()
        uname = event.get_sender_name()
        self._apply_decay(uid, uname)
        length = self.db.get_user_length(uid)
        plain_text = f"📏 {uname} 当前长度：{self._fmt_len(length)} cm"
        if await self._reply_group(
            event,
            plain_text,
            self._format_qqofficial_length(uname, length),
        ):
            return
        yield event.plain_result(plain_text)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("lu")
    async def lu_guan(self, event: AstrMessageEvent):
        """鹿关"""
        uid = event.get_sender_id()
        uname = event.get_sender_name()

        try:
            current_len = self.db.get_user_length(uid)
            if current_len <= 0:
                plain_text = "⚠️ 当前长度为 0，无法鹿关，请先使用 /growth。"
                if await self._reply_group(
                    event,
                    plain_text,
                    self._format_qqofficial_notice("无法降落", "当前长度为 0，请先使用 /growth。"),
                ):
                    return
                yield event.plain_result(plain_text)
                return

            date_str = datetime.now().strftime("%Y-%m-%d")
            lu_count = self.db.increment_daily_lu(uid, date_str)
            lu_length, milk_amount, pressure = self._roll_lu_outcome(
                current_len, lu_count
            )
            new_len = round(max(0.0, current_len - lu_length), 2)
            self.db.update_user_length(uid, uname, new_len)
            self.db.adjust_user_milk(uid, milk_amount, uname)

            fatigue_msg = ""
            if pressure > 0:
                fatigue_msg = f"\n😵 疲劳惩罚：第 {lu_count} 次，收益下滑、消耗上升"
            plain_text = (
                f"🦌 机长 {uname} 已成功降落，当前长度：{self._fmt_len(new_len)} cm\n"
                f"🥛 产生金液 {self._fmt_len(milk_amount)} ml"
                f"{fatigue_msg}"
            )
            if await self._reply_group(
                event,
                plain_text,
                self._format_qqofficial_lu_result(uname, new_len, milk_amount, lu_count, pressure),
            ):
                return
            yield event.plain_result(plain_text)
        except Exception as e:
            logger.error(f"Lu Error: {e}")
            plain_text = "手抽筋儿了，停止起飞"
            if await self._reply_group(
                event,
                plain_text,
                self._format_qqofficial_notice("降落失败", plain_text),
            ):
                return
            yield event.plain_result(plain_text)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("milk")
    async def show_milk(self, event: AstrMessageEvent):
        """查询金液"""
        uid = event.get_sender_id()
        uname = event.get_sender_name()
        milk_ml = self.db.get_user_milk(uid)
        plain_text = f"🥛 {uname} 当前金液：{self._fmt_len(milk_ml)} ml"
        if await self._reply_group(
            event,
            plain_text,
            self._format_qqofficial_milk(uname, milk_ml),
        ):
            return
        yield event.plain_result(plain_text)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("pvp")
    async def pvp_start(self, event: AstrMessageEvent):
        """发起挑战"""
        gid = self.get_gid(event)
        args = event.message_str.strip().split()

        if len(args) < 2:
            plain_text = "⚠️ 格式错误。正确用法：/pvp [赌注长度]"
            if await self._reply_group(
                event,
                plain_text,
                self._format_qqofficial_notice("格式错误", "正确用法：/pvp [赌注长度]"),
            ):
                return
            yield event.plain_result(plain_text)
            return

        try:
            bet = round(float(args[1]), 2)
            if bet <= 0:
                raise ValueError
        except ValueError:
            plain_text = "❌ 赌注必须是正数。"
            if await self._reply_group(
                event,
                plain_text,
                self._format_qqofficial_notice("赌注无效", "赌注必须是正数。"),
            ):
                return
            yield event.plain_result(plain_text)
            return

        uid = event.get_sender_id()
        uname = event.get_sender_name()
        self._apply_decay(uid, uname)
        u_len = self.db.get_user_length(uid)

        if u_len < bet:
            plain_text = f"你的长度不足 {bet} cm，无法发起如此豪赌！(当前: {u_len} cm)"
            if await self._reply_group(
                event,
                plain_text,
                self._format_qqofficial_notice(
                    "长度不足",
                    f"需要 {self._fmt_len(bet)} cm，当前 {self._fmt_len(u_len)} cm。",
                ),
            ):
                return
            yield event.plain_result(plain_text)
            return

        plain_pvp_prompt = (
            f"🔥 {uname} 开启了 {bet} cm 的决斗！\n谁敢一战？回复 /comeon 加入战斗。"
        )
        qqofficial_prompt = self._format_qqofficial_pvp_prompt(uname, bet, u_len)

        async def send_pvp_timeout_notice():
            plain_text = f"💤 {uname} 的挑战因无人应战已作废。"
            if await self._send_qqofficial_markdown(
                event,
                self._format_qqofficial_notice("决斗作废", f"{uname} 的挑战因无人应战已作废。"),
            ):
                return
            await event.send(event.plain_result(plain_text))

        # 清理该群旧对局
        await self.cancel_existing_task(gid)

        # 超时闭包
        async def pvp_timeout(seconds: int):
            try:
                await asyncio.sleep(seconds)
                if gid in self.active_challenges:
                    del self.active_challenges[gid]
                    await send_pvp_timeout_notice()
            except asyncio.CancelledError:
                pass

        # 记录状态
        self.active_challenges[gid] = {
            "data": {
                "initiator_id": uid,
                "initiator_name": uname,
                "initiator_length": u_len,
                "bet": bet,
            },
            "task": asyncio.create_task(pvp_timeout(self.pvp_timeout_seconds)),
        }

        if await self._send_qqofficial_pvp_button(event, gid, qqofficial_prompt):
            event.stop_event()
            return
        if await self._send_qqofficial_markdown(event, qqofficial_prompt):
            event.stop_event()
            return

        yield event.plain_result(plain_pvp_prompt)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("comeon")
    async def pvp_join(self, event: AstrMessageEvent):
        """响应挑战"""
        gid = self.get_gid(event)

        if gid not in self.active_challenges:
            return  # 保持安静，不干扰正常聊天

        p_id = event.get_sender_id()
        p_name = event.get_sender_name()
        result_text = await self._join_pvp(
            gid,
            p_id,
            p_name,
            use_markdown=_is_qqofficial_event(event),
        )
        if result_text is not None:
            if await self._reply_group(event, result_text, result_text):
                return
            yield event.plain_result(result_text)

    async def terminate(self):
        # 卸载时清理所有协程
        for gid in list(self.active_challenges.keys()):
            await self.cancel_existing_task(gid)
        if hasattr(self, "db"):
            self.db.close()
