from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
import random
import os
import asyncio
import math
from datetime import datetime

@register("dickfighting", "letr", "斗鸡插件", "0.0.6")
class MyPlugin(Star):
    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self.config = config or {}
        self._load_settings()
        self.active_challenges = {}  # {group_id: {"data": dict, "task": Task}}

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

        self.decay_enable = bool(self._get_config_value("decay", "enable", default=False))
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
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path
        from .db import Database
        
        # 确保数据目录存在
        plugin_data_path = f"{get_astrbot_data_path()}/plugin_data/{self.name}"
        os.makedirs(plugin_data_path, exist_ok=True)
        
        # 初始化数据库
        self.db = Database(f"{plugin_data_path}/data.db")

    def get_gid(self, event: AstrMessageEvent):
        """安全获取群组ID标识"""
        try:
            return event.get_group_id() or f"private_{event.get_sender_id()}"
        except:
            return str(event.get_sender_id())

    async def cancel_existing_task(self, gid: str):
        """清理并取消现有的超时任务"""
        if gid in self.active_challenges:
            task = self.active_challenges[gid].get("task")
            if task and not task.done():
                task.cancel()
            del self.active_challenges[gid]

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
                yield event.plain_result(
                        f"今天的锻炼已达到上限 ({self.growth_daily_limit} 次)，请明天再来。"
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
            
            yield event.plain_result(f"✨ {uname} 进行了晨间锻炼！\n长度增加了 {growth_amount} cm，当前总量：{new_len} cm。")
        except Exception as e:
            logger.error(f"Growth Error: {e}")
            yield event.plain_result("锻炼时抽筋了，请稍后再试。")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("pvp")
    async def pvp_start(self, event: AstrMessageEvent):
        """发起挑战"""
        gid = self.get_gid(event)
        args = event.message_str.strip().split()
        
        if len(args) < 2:
            yield event.plain_result("⚠️ 格式错误。正确用法：/pvp [赌注长度]")
            return
            
        try:
            bet = round(float(args[1]), 2)
            if bet <= 0: raise ValueError
        except ValueError:
            yield event.plain_result("❌ 赌注必须是正数。")
            return
        
        uid = event.get_sender_id()
        uname = event.get_sender_name()
        self._apply_decay(uid, uname)
        u_len = self.db.get_user_length(uid)

        if u_len < bet:
            yield event.plain_result(f"你的长度不足 {bet} cm，无法发起如此豪赌！(当前: {u_len} cm)")
            return
        
        # 清理该群旧对局
        await self.cancel_existing_task(gid)

        # 超时闭包
        async def pvp_timeout(seconds: int):
            try:
                await asyncio.sleep(seconds)
                if gid in self.active_challenges:
                    del self.active_challenges[gid]
                    await event.send(event.plain_result(f"💤 {uname} 的挑战因无人应战已作废。"))
            except asyncio.CancelledError:
                pass 

        # 记录状态
        self.active_challenges[gid] = {
            "data": {
                "initiator_id": uid,
                "initiator_name": uname,
                "initiator_length": u_len,
                "bet": bet
            },
            "task": asyncio.create_task(pvp_timeout(self.pvp_timeout_seconds))
        }

        yield event.plain_result(f"🔥 {uname} 开启了 {bet} cm 的决斗！\n谁敢一战？回复 /comeon 加入战斗。")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("comeon")
    async def pvp_join(self, event: AstrMessageEvent):
        """响应挑战"""
        gid = self.get_gid(event)
        
        if gid not in self.active_challenges:
            return # 保持安静，不干扰正常聊天

        challenge_data = self.active_challenges[gid]["data"]
        p_id = event.get_sender_id()
        p_name = event.get_sender_name()
        self._apply_decay(p_id, p_name)

        if p_id == challenge_data["initiator_id"]:
            yield event.plain_result("你不能左右互搏！")
            return

        p_len = self.db.get_user_length(p_id)
        bet = challenge_data["bet"]

        if p_len < bet:
            yield event.plain_result(f"你的长度不足 {bet} cm，去锻炼一下再来吧。")
            return

        # 成功接战，立即取消超时提醒任务
        await self.cancel_existing_task(gid)

        try:
            i_len = challenge_data["initiator_length"]
            
            # 胜率计算逻辑：先对长度做 log 缩放，削弱超长带来的差距
            # $$P_1 = \frac{log(1+L_1)^a}{log(1+L_1)^a + log(1+L_2)^a}$$
            pow_a = self.win_prob_power
            i_base = math.log1p(max(i_len, self.win_prob_min_length))
            p_base = math.log1p(max(p_len, self.win_prob_min_length))
            i_p = math.pow(i_base, pow_a)
            p_p = math.pow(p_base, pow_a)
            win_prob = i_p / (i_p + p_p)
            
            is_init_win = random.random() < win_prob
            
            if is_init_win:
                win_id, win_name = challenge_data["initiator_id"], challenge_data["initiator_name"]
                lose_id, lose_name = p_id, p_name
            else:
                win_id, win_name = p_id, p_name
                lose_id, lose_name = challenge_data["initiator_id"], challenge_data["initiator_name"]

            winner_prob = win_prob if is_init_win else 1 - win_prob
            odds = self._calc_odds(winner_prob)
            effective_bet = round(bet * odds, 2)
            max_loss = min(i_len, p_len)
            if effective_bet > max_loss:
                effective_bet = round(max_loss, 2)

            # 结算
            self.db.adjust_user_length(win_id, effective_bet)
            self.db.adjust_user_length(lose_id, -effective_bet)
            
            res_win = self.db.get_user_length(win_id)
            res_lose = self.db.get_user_length(lose_id)

            yield event.plain_result(
                f"⚔️ 决斗结束！\n"
                f"━━━━━━━━━━━━━━\n"
                f"👑 胜者：{win_name} (+{effective_bet}cm)\n"
                f"💀 败者：{lose_name} (-{effective_bet}cm)\n"
                f"🎲 赔率：{odds}x\n"
                f"━━━━━━━━━━━━━━\n"
                f"📊 战报：{win_name}({res_win}cm) | {lose_name}({res_lose}cm)"
            )
        except Exception as e:
            logger.error(f"PVP Logic Error: {e}")
            yield event.plain_result("计算对局时服务器卡了一下，双方各回各家。")

    async def terminate(self):
        # 卸载时清理所有协程
        for gid in list(self.active_challenges.keys()):
            await self.cancel_existing_task(gid)
        if hasattr(self, "db"):
            self.db.close()


