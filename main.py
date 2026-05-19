import json
import os
import asyncio
import random
import discord
from discord.ext import commands

# --- CẤU HÌNH ---
TOKEN = os.getenv("DISCORD_TOKEN")
prefix = "!"
checkpoint_file = "checkpoints_multi.json"
channels_file = "channels.txt"

intents = discord.Intents.all()
bot = commands.Bot(command_prefix=prefix,
                   help_command=None,
                   intents=intents,
                   self_bot=True)

TOTAL_REACT_LIMIT = 15000
auto_react_enabled = True
reaction_queue = asyncio.Queue()
is_cleaning = False

# --- HÀM QUẢN LÝ DỮ LIỆU BẤT ĐỒNG BỘ ---
def _sync_load_data():
    default_data = {"checkpoints": {}, "stats": {"current_total": 0, "limit": TOTAL_REACT_LIMIT}}
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "stats" not in data: data["stats"] = default_data["stats"]
                if "checkpoints" not in data: data["checkpoints"] = {}
                return data
        except:
            return default_data
    return default_data

def _sync_save_data(data):
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def _sync_load_channels():
    if not os.path.exists(channels_file):
        with open(channels_file, "w", encoding="utf-8") as f: pass
        return []
    with open(channels_file, "r", encoding="utf-8") as f:
        return [int(line.strip()) for line in f if line.strip() and not line.startswith("#")]

async def save_all_data():
    data = {
        "checkpoints": channel_checkpoints,
        "stats": {
            "current_total": current_total_reacts,
            "limit": TOTAL_REACT_LIMIT
        }
    }
    await asyncio.to_thread(_sync_save_data, data)

# Khởi tạo dữ liệu
data_store = _sync_load_data()
channel_checkpoints = data_store["checkpoints"]
current_total_reacts = data_store["stats"]["current_total"]
TOTAL_REACT_LIMIT = data_store["stats"]["limit"]
TARGET_CHANNELS = _sync_load_channels()

# --- HÀM REACT CỐT LÕI ---
async def smart_react(msg, channel_id):
    global current_total_reacts

    if not auto_react_enabled or current_total_reacts >= TOTAL_REACT_LIMIT:
        return

    my_reactions = [str(r.emoji) for r in msg.reactions if r.me]
    missing_reactions = [r for r in msg.reactions if str(r.emoji) not in my_reactions]

    if not missing_reactions: 
        return

    num_to_react = min(len(missing_reactions), TOTAL_REACT_LIMIT - current_total_reacts)
    reactions_to_add = random.sample(missing_reactions, num_to_react)

    for reaction in reactions_to_add:
        try:
            await msg.add_reaction(reaction.emoji)
            current_total_reacts += 1
            print(f"[{channel_id}] ✨ Đã thả: {current_total_reacts}/{TOTAL_REACT_LIMIT}")
            await asyncio.sleep(random.uniform(0.8, 1.4)) # Tốc độ tối ưu an toàn
        except Exception as e:
            print(f"⚠️ Lỗi thả emoji tại kênh {channel_id}: {e}")
            break

    await save_all_data()

# --- WORKER VÀ EVENT ---
async def reaction_worker():
    while True:
        try:
            msg = await reaction_queue.get()

            while is_cleaning:
                await asyncio.sleep(1)

            if auto_react_enabled and current_total_reacts < TOTAL_REACT_LIMIT:
                await smart_react(msg, msg.channel.id)
                await asyncio.sleep(random.uniform(0.8, 1.5)) # Nghỉ ngắn giữa các tin nhắn để giữ nhịp nhanh
        except Exception as e:
            print(f"❌ Lỗi Worker ngầm: {e}")
        finally:
            reaction_queue.task_done()

@bot.event
async def on_message(message):
    if not auto_react_enabled or message.channel.id not in TARGET_CHANNELS:
        await bot.process_commands(message)
        return

    async def wait_and_push(m):
        # Giảm thời gian chờ bắt tin nhắn mới xuống 5-8s để đẩy vào hàng đợi nhanh hơn cho 36 kênh
        await asyncio.sleep(random.uniform(5, 8))
        try:
            refreshed_msg = await m.channel.fetch_message(m.id)
            if refreshed_msg.reactions:
                await reaction_queue.put(refreshed_msg)
        except:
            pass

    bot.loop.create_task(wait_and_push(message))
    await bot.process_commands(message)

# --- LỆNH ĐIỀU KHIỂN NÂNG CẤP CHO 36 KÊNH ---
@bot.command(aliases=["clean"])
async def follow_old(ctx):
    global is_cleaning
    try: await ctx.message.delete()
    except: pass
    if not auto_react_enabled: return

    is_cleaning = True
    print(f"🧹 [HỆ THỐNG] ĐANG TIẾN HÀNH THU THẬP TIN NHẮN TỪ {len(TARGET_CHANNELS)} KÊNH...")

    temp_msg_list = [] # Danh sách tạm để gom tin nhắn của tất cả các kênh

    for cid in TARGET_CHANNELS:
        # Nếu đã đạt giới hạn ngay trong lúc quét thì dừng gom luôn cho nhẹ máy
        if len(temp_msg_list) >= (TOTAL_REACT_LIMIT - current_total_reacts):
            break

        channel = bot.get_channel(cid)
        if not channel: continue

        last_id = channel_checkpoints.get(str(cid), {}).get("last_id")
        # Chia nhỏ limit mỗi kênh xuống 150 tin (thay vì 500) để phân phối đều và không làm ngợp RAM Railway
        args = {"limit": 150} 
        if last_id: args["before"] = discord.Object(id=int(last_id))

        try:
            async for msg in channel.history(**args):
                if msg.reactions:
                    temp_msg_list.append(msg)
                channel_checkpoints[str(cid)] = {"last_id": str(msg.id)}
        except Exception as e:
            print(f"❌ Lỗi khi quét kênh {cid}: {e}")

    # THUẬT TOÁN QUAN TRỌNG: Trộn đều toàn bộ tin nhắn của 36 kênh lại với nhau
    print(f"🔄 Đang trộn ngẫu nhiên {len(temp_msg_list)} tin nhắn thu thập được để chia đều React...")
    random.shuffle(temp_msg_list)

    # Đẩy danh sách đã trộn vào hàng đợi chính cho Worker xử lý
    for msg in temp_msg_list:
        await reaction_queue.put(msg)

    await save_all_data()
    is_cleaning = False
    print(f"🏁 ĐÃ PHÂN BỔ XONG HÀNG ĐỢI. Worker bắt đầu thả đều cho các kênh...")

@bot.command()
async def total(ctx, num: int):
    global TOTAL_REACT_LIMIT
    TOTAL_REACT_LIMIT = num
    await save_all_data()
    try: await ctx.message.delete()
    except: pass
    print(f"♻️ Hạn mức mới: {num}")

@bot.command()
async def reload(ctx):
    global TARGET_CHANNELS
    try:
        TARGET_CHANNELS = await asyncio.to_thread(_sync_load_channels)
        try: await ctx.message.delete()
        except: pass
        print(f"🔄 ĐÃ CẬP NHẬT: Hiện có {len(TARGET_CHANNELS)} kênh trong danh sách.")
    except Exception as e:
        print(f"❌ Lỗi khi reload: {e}")

@bot.command()
async def start(ctx):
    global auto_react_enabled
    auto_react_enabled = True
    try: await ctx.message.delete()
    except: pass
    print("▶️ BẬT AUTO REACT")

@bot.command()
async def stop(ctx):
    global auto_react_enabled
    auto_react_enabled = False
    await save_all_data()
    try: await ctx.message.delete()
    except: pass
    print("⛔ DỪNG AUTO REACT")

@bot.event
async def on_ready():
    bot.loop.create_task(reaction_worker())
    print(f"✅ Bot Online | Tiến độ: {current_total_reacts}/{TOTAL_REACT_LIMIT} | Đang quản lý: {len(TARGET_CHANNELS)} kênh.")

bot.run(TOKEN, bot=False)
