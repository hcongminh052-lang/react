import json
import os
import asyncio
import random
import time
import discord
from discord.ext import commands
from flask import Flask
from threading import Thread
import logging

PRIORITY_USERS = [1335190890930769960]

# =====================================================================
# 🛠️ 1. KEEPALIVE SERVER
# =====================================================================
app = Flask('')
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route('/')
def home(): 
    return "OK", 200

def run_flask():
    port = int(os.environ.get("PORT", random.randint(12000, 25000)))
    while True:
        try:
            print(f"📡 [SERVER] Khởi chạy Keep-Alive Server tại cổng tự do: {port}...", flush=True)
            app.run(host='0.0.0.0', port=port, threaded=False, use_reloader=False)
            break
        except Exception as e:
            port = random.randint(12000, 25000)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()

# =====================================================================
# ⚙️ 2. CẤU HÌNH BIẾN TOÀN CỤC VÀ HỆ THỐNG CHECKPOINT
# =====================================================================
TOKEN = os.getenv("DISCORD_TOKEN")
prefix = "!"

checkpoint_file = "/app/data/checkpoints_multi.json"
channels_file = "/app/data/channels.txt"
backup_file = "backup_channels.txt"

intents = discord.Intents.all()
bot = commands.Bot(command_prefix=prefix, help_command=None, intents=intents, self_bot=True)

TOTAL_REACT_LIMIT = 999999
current_total_reacts = 0
auto_react_enabled = False 
reaction_queue = asyncio.Queue()
is_cleaning = False
channel_checkpoints = {}  

failed_channels_pool = {}  

loop_manager_task = None
worker_task = None
bg_mining_task = None  

def _sync_load_data():
    default_data = {"stats": {"current_total": 0, "limit": TOTAL_REACT_LIMIT}, "checkpoints": {}}
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
    try:
        os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e: 
        print(f"❌ Lỗi ghi file checkpoint: {e}", flush=True)

def _sync_load_channels():
    os.makedirs(os.path.dirname(channels_file), exist_ok=True)
    if not os.path.exists(channels_file):
        with open(channels_file, "w", encoding="utf-8") as f: pass
    with open(channels_file, "r", encoding="utf-8") as f:
        channels = [int(line.strip()) for line in f if line.strip() and not line.startswith("#")]
    if not channels and os.path.exists(backup_file):
        with open(backup_file, "r", encoding="utf-8") as f_backup: 
            backup_content = f_backup.read()
        with open(channels_file, "w", encoding="utf-8") as f_volume: 
            f_volume.write(backup_content)
        channels = [int(line.strip()) for line in backup_content.split("\n") if line.strip() and not line.startswith("#")]
    return channels

async def save_all_data():
    data = {
        "stats": {"current_total": current_total_reacts, "limit": TOTAL_REACT_LIMIT},
        "checkpoints": channel_checkpoints 
    }
    await asyncio.to_thread(_sync_save_data, data)

data_store = _sync_load_data()
current_total_reacts = data_store["stats"]["current_total"]
TOTAL_REACT_LIMIT = data_store["stats"]["limit"]
channel_checkpoints = data_store.get("checkpoints", {})
TARGET_CHANNELS = _sync_load_channels()

# =====================================================================
# ⚡ 3. HÀM THẢ REACT SIÊU TỐC VÀ ĐÁNH ĐIỂM PHẠT KÊNH LỖI
# =====================================================================
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
            print(f"[{channel_id}] ✨ Đã thả: {current_total_reacts}/{TOTAL_REACT_LIMIT}", flush=True)
            await asyncio.sleep(random.uniform(0.3, 0.6)) 
        except (discord.errors.HTTPException, discord.errors.InvalidArgument, Exception):
            print(f"⚠️ Bỏ qua tác vụ lỗi tại kênh {channel_id}", flush=True)
            if channel_id not in failed_channels_pool:
                failed_channels_pool[channel_id] = {"count": 1, "timeout": 0}
            else:
                failed_channels_pool[channel_id]["count"] += 1
                
            if failed_channels_pool[channel_id]["count"] >= 3:
                failed_channels_pool[channel_id]["timeout"] = time.time() + 1800
                print(f"🚫 [DANH SÁCH ĐEN] Kênh {channel_id} lỗi liên tiếp. Khóa quét 30 phút!", flush=True)
            break

    if current_total_reacts % 30 == 0:
        await save_all_data()

# =====================================================================
# 📦 4. WORKER NGẦM XỬ LÝ HÀNG ĐỢI
# =====================================================================
async def reaction_worker():
    try:
        while True:
            msg = await reaction_queue.get()
            while is_cleaning:
                await asyncio.sleep(0.5)

            if auto_react_enabled and current_total_reacts < TOTAL_REACT_LIMIT:
                await smart_react(msg, msg.channel.id)
                
            reaction_queue.task_done()
    except asyncio.CancelledError:
        print("📥 [WORKER] Luồng xử lý hàng đợi thả react đã bị hủy hoàn toàn.", flush=True)

# =====================================================================
# 🧹 5. LUỒNG QUÉT CHÍNH DIỆN (MAIN FOREGROUND CLEANER)
# =====================================================================
async def follow_old_logic(target_per_channel=40):
    global is_cleaning, channel_checkpoints
    if not auto_react_enabled: return

    is_cleaning = True
    print(f"🧹 [HỆ THỐNG] Tiến hành ĐÀO SÂU liên tục về bài cũ...", flush=True)
    global_temp_list = []

    shuffled_channels = TARGET_CHANNELS.copy()
    random.shuffle(shuffled_channels)

    try:
        for cid in shuffled_channels:
            if current_total_reacts >= TOTAL_REACT_LIMIT or not auto_react_enabled:
                break

            if cid in failed_channels_pool and time.time() < failed_channels_pool[cid]["timeout"]:
                continue

            channel = bot.get_channel(cid)
            if not channel: 
                failed_channels_pool[cid] = {"count": 1, "timeout": time.time() + 7200}
                continue

            oldest_msg_id = channel_checkpoints.get(str(cid), {}).get("last_id")
            if oldest_msg_id == "BOTTOM_REACHED":
                continue
                
            channel_gathered = 0
            current_cursor_id = int(oldest_msg_id) if oldest_msg_id else None
            consecutive_empty_chunks = 0  

            while channel_gathered < target_per_channel and auto_react_enabled:
                args = {"limit": 200} 
                if current_cursor_id:
                    args["before"] = discord.Object(id=current_cursor_id)

                history_chunk = []
                try:
                    async for msg in channel.history(**args): 
                        history_chunk.append(msg)
                except Exception: 
                    failed_channels_pool[cid] = {"count": 1, "timeout": time.time() + 7200}
                    break 

                if not history_chunk: 
                    print(f"🎉 Kênh {cid} đã được đào tận gốc lịch sử!", flush=True)
                    channel_checkpoints[str(cid)] = {"last_id": "BOTTOM_REACHED"}
                    break

                current_cursor_id = history_chunk[-1].id
                channel_checkpoints[str(cid)] = {"last_id": str(current_cursor_id)} # ÉP LƯU CHECKPOINT NGAY LẬP TỨC
                
                valid_count_in_chunk = 0
                for msg in history_chunk:
                    if msg.reactions:
                        my_reactions = [str(r.emoji) for r in msg.reactions if r.me]
                        missing_reactions = [r for r in msg.reactions if str(r.emoji) not in my_reactions]
                        
                        if missing_reactions:
                            is_vip = msg.author.id in PRIORITY_USERS
                            global_temp_list.append((msg, is_vip))
                            channel_gathered += 1
                            valid_count_in_chunk += 1
                            if channel_gathered >= target_per_channel: 
                                break
                                
                del history_chunk
                
                if valid_count_in_chunk == 0:
                    consecutive_empty_chunks += 1
                    if consecutive_empty_chunks % 5 == 0:
                        print(f"   ↳ [XUYÊN THẤU - CHÍNH] Kênh {cid} lội qua {consecutive_empty_chunks*200} bài trống. Tới ID: {current_cursor_id}", flush=True)
                    await asyncio.sleep(0.4) 
                else:
                    consecutive_empty_chunks = 0  
                    await asyncio.sleep(0.2)

        await save_all_data()

        if global_temp_list and auto_react_enabled:
            priority_list = [item[0] for item in global_temp_list if item[1] is True]
            normal_list = [item[0] for item in global_temp_list if item[1] is False]
            random.shuffle(priority_list); random.shuffle(normal_list)
            
            for msg in (priority_list + normal_list):
                await reaction_queue.put(msg)
            print(f"🔄 CHÍNH: Gom thành công {len(global_temp_list)} bài. Hàng đợi hiện tại: {reaction_queue.qsize()}", flush=True)

    except Exception as e:
        print(f"🚨 Lỗi luồng quét chính: {e}", flush=True)
    finally:
        is_cleaning = False

# =====================================================================
# 💤 6. LUỒNG KHAI THÁC NGẦM ĐỘC LẬP KHI BOT NGHỈ (BACKGROUND MINER)
# =====================================================================
async def background_mining_worker(sleep_duration):
    """Tách biệt hoàn toàn, tự động dịch chuyển mốc và lưu dữ liệu liên tục"""
    print(f"⛏️ [MINER NGẦM] Bắt đầu đào dữ liệu lịch sử trong {sleep_duration}s nghỉ...", flush=True)
    end_time = time.time() + sleep_duration
    
    try:
        while time.time() < end_time and auto_react_enabled:
            if reaction_queue.qsize() >= 150:
                await asyncio.sleep(2)
                continue

            shuffled_channels = TARGET_CHANNELS.copy()
            random.shuffle(shuffled_channels)
            bg_gathered = 0

            for cid in shuffled_channels:
                if time.time() >= end_time or not auto_react_enabled or reaction_queue.qsize() >= 150:
                    break

                if cid in failed_channels_pool and time.time() < failed_channels_pool[cid]["timeout"]:
                    continue

                channel = bot.get_channel(cid)
                if not channel: continue

                oldest_msg_id = channel_checkpoints.get(str(cid), {}).get("last_id")
                if oldest_msg_id == "BOTTOM_REACHED": continue

                current_cursor_id = int(oldest_msg_id) if oldest_msg_id else None
                
                # Miner chỉ cào lướt 2 khối tin nhắn (400 bài) rồi đổi kênh liên tục để tăng độ phủ rộng
                for _ in range(2):
                    if time.time() >= end_time or not auto_react_enabled: break
                    
                    args = {"limit": 200}
                    if current_cursor_id:
                        args["before"] = discord.Object(id=current_cursor_id)

                    chunk = []
                    try:
                        async for msg in channel.history(**args): chunk.append(msg)
                    except: break

                    if not chunk:
                        channel_checkpoints[str(cid)] = {"last_id": "BOTTOM_REACHED"}
                        break

                    current_cursor_id = chunk[-1].id
                    channel_checkpoints[str(cid)] = {"last_id": str(current_cursor_id)} # ĐƯỢC LƯU NGAY

                    for msg in chunk:
                        if msg.reactions:
                            my_reacts = [str(r.emoji) for r in msg.reactions if r.me]
                            missing = [r for r in msg.reactions if str(r.emoji) not in my_reacts]
                            if missing:
                                await reaction_queue.put(msg)
                                bg_gathered += 1
                    
                    del chunk
                    await asyncio.sleep(1.2) # Nhịp độ ~1000 tin nhắn/phút cực kỳ chuẩn xác

            if bg_gathered > 0:
                print(f"📦 [MINER NGẦM] Đã tích trữ thêm {bg_gathered} bài vào kho dự trữ ngầm. (Kho: {reaction_queue.qsize()})", flush=True)
                await save_all_data()
            
            await asyncio.sleep(1)
    except Exception as e:
        print(f"🚨 Lỗi luồng đào ngầm: {e}", flush=True)

# =====================================================================
# 🔄 7. BỘ QUẢN LÝ VÒNG LẶP TUẦN TỰ (SMART LOOP MANAGER)
# =====================================================================
async def auto_loop_manager():
    global bg_mining_task
    try:
        while True:
            if auto_react_enabled:
                print("\n🔄 [LOOP MANAGER] === BẮT ĐẦU MỘT CHU KỲ QUÉT MỎ MỚI ===", flush=True)
                start_reacts = current_total_reacts
                
                if reaction_queue.empty():
                    print("🔍 [HỆ THỐNG] Hàng đợi trống. Tiến hành quét tìm bài...", flush=True)
                    await follow_old_logic(target_per_channel=40)
                else:
                    print(f"⚡ [HỆ THỐNG] Hàng đợi đã được tích lũy sẵn {reaction_queue.qsize()} bài từ trước! Bung lụa react ngay...", flush=True)
                
                # Chờ xử lý hết bài trong hàng đợi
                while not reaction_queue.empty() and auto_react_enabled:
                    await asyncio.sleep(1)
                
                reacts_gained = current_total_reacts - start_reacts
                
                if reacts_gained > 0:
                    sleep_time = 15
                    print(f"⚡ [HỆ THỐNG] Lượt vừa rồi cày được {reacts_gained} react. Chuyển sang luồng ĐÀO NGẦM.", flush=True)
                    bg_mining_task = bot.loop.create_task(background_mining_worker(sleep_time))
                    await asyncio.sleep(sleep_time)
                else:
                    sleep_time = 600
                    print("💤 Toàn bộ mỏ hiện tại đã cạn hoặc lỗi. Hệ thống ngủ 10 phút để ĐÀO NGẦM xuyên phá...", flush=True)
                    bg_mining_task = bot.loop.create_task(background_mining_worker(sleep_time))
                    await asyncio.sleep(sleep_time)
            else:
                await asyncio.sleep(1)
    except asyncio.CancelledError:
        print("🔄 [LOOP MANAGER] Luồng quản lý vòng lặp đã bị tắt.", flush=True)

# --- 🔥 HỆ THỐNG LỆNH ĐIỀU KHIỂN ---
@bot.command()
async def start(ctx):
    try: await ctx.message.delete()
    except: pass
    global auto_react_enabled, loop_manager_task, worker_task
    if auto_react_enabled: return

    auto_react_enabled = True
    print("▶️ [KÍCH HOẠT] ĐANG BẬT LUỒNG HỆ THỐNG AUTO REACT...", flush=True)
    
    while not reaction_queue.empty():
        try: reaction_queue.get_nowait()
        except: break

    worker_task = bot.loop.create_task(reaction_worker())
    loop_manager_task = bot.loop.create_task(auto_loop_manager())

@bot.command()
async def stop(ctx):
    try: await ctx.message.delete()
    except: pass
    global auto_react_enabled, loop_manager_task, worker_task, bg_mining_task, is_cleaning
    if not auto_react_enabled: return

    auto_react_enabled = False
    is_cleaning = False
    print("⛔ [DỪNG KHẨN CẤP] ĐANG KHAI TỬ TOÀN BỘ LUỒNG AUTO REACT VÀ MINER...", flush=True)
    
    if loop_manager_task and not loop_manager_task.done(): loop_manager_task.cancel()
    if worker_task and not worker_task.done(): worker_task.cancel()
    if bg_mining_task and not bg_mining_task.done(): bg_mining_task.cancel()
        
    loop_manager_task = None
    worker_task = None
    bg_mining_task = None

    while not reaction_queue.empty():
        try: reaction_queue.get_nowait()
        except: break

    await save_all_data()
    print("✅ Đã đưa hệ thống về trạng thái ĐÓNG BĂNG HOÀN TOÀN thành công.", flush=True)

@bot.command()
async def total(ctx, num: int):
    global TOTAL_REACT_LIMIT
    TOTAL_REACT_LIMIT = num
    await save_all_data()
    print(f"♻️ Hạn mức mới: {num}", flush=True)

@bot.command()
async def reload(ctx):
    global TARGET_CHANNELS
    try:
        TARGET_CHANNELS = await asyncio.to_thread(_sync_load_channels)
        print(f"🔄 ĐÃ CẬP NHẬT: Hiện có {len(TARGET_CHANNELS)} kênh.", flush=True)
    except Exception as e:
        print(f"❌ Lỗi reload danh sách kênh: {e}", flush=True)

@bot.command()
async def reset(ctx):
    try: await ctx.message.delete()
    except: pass
    global channel_checkpoints, failed_channels_pool
    channel_checkpoints = {}
    failed_channels_pool = {} 
    await save_all_data()
    print("🧹 [HỆ THỐNG] Đã xóa sạch toàn bộ dữ liệu checkpoint và giải phóng kênh lỗi!", flush=True)

@bot.event
async def on_ready():
    print(f"✅ Bot Online (Môi trường Railway Cloud) | Tiến độ hiện tại: {current_total_reacts}/{TOTAL_REACT_LIMIT}", flush=True)

keep_alive()
try:
    bot.run(TOKEN, bot=False, reconnect=True)
except Exception as e:
    print(f"❌ Lỗi kết nối Gateway: {e}", flush=True)
