from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
import random
import os
import asyncio
import math

@register("dickfighting", "letr", "斗鸡插件", "0.0.4")
class MyPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.active_challenges = {}  # {group_id: {"data": dict, "task": Task}}

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
        
        # 随机增长量
        growth_amount = round(random.uniform(0.1, 5.0), 2)
        
        try:
            # 这里的 update_user_length 逻辑应包含：若不存在则初始化，若存在则累加
            current_len = self.db.get_user_length(uid)
            new_len = round(current_len + growth_amount, 2)
            self.db.update_user_length(uid, uname, new_len)
            
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
            "task": asyncio.create_task(pvp_timeout(60))
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
            
            # 胜率计算逻辑：使用幂函数增强弱者胜率
            # $$P_1 = \frac{L_1^{0.7}}{L_1^{0.7} + L_2^{0.7}}$$
            pow_a = 0.7
            i_p = math.pow(max(i_len, 0.1), pow_a)
            p_p = math.pow(max(p_len, 0.1), pow_a)
            win_prob = i_p / (i_p + p_p)
            
            is_init_win = random.random() < win_prob
            
            if is_init_win:
                win_id, win_name = challenge_data["initiator_id"], challenge_data["initiator_name"]
                lose_id, lose_name = p_id, p_name
            else:
                win_id, win_name = p_id, p_name
                lose_id, lose_name = challenge_data["initiator_id"], challenge_data["initiator_name"]

            # 结算
            self.db.adjust_user_length(win_id, bet)
            self.db.adjust_user_length(lose_id, -bet)
            
            res_win = self.db.get_user_length(win_id)
            res_lose = self.db.get_user_length(lose_id)

            yield event.plain_result(
                f"⚔️ 决斗结束！\n"
                f"━━━━━━━━━━━━━━\n"
                f"👑 胜者：{win_name} (+{bet}cm)\n"
                f"💀 败者：{lose_name} (-{bet}cm)\n"
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