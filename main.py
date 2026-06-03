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
# 🛠️ 1. KEEPALIVE SERVER (KHÔNG DÙNG CỔNG CỐ ĐỊNH CHỐNG CRASH)
# =====================================================================
app = Flask('')
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route('/')
def home(): 
    return "OK", 200

def run_flask():
    port = int(os.environ.get("PORT", random.randint(12000, 25000)))
    
    import logging
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    
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
            await asyncio.sleep(random.uniform(0.25, 0.5)) 
        except discord.errors.HTTPException as e:
            if e.code == 10014:
                print(f"⚠️ Bỏ qua emoji lỗi 10014 tại kênh {channel_id}", flush=True)
                if channel_id not in failed_channels_pool:
                    failed_channels_pool[channel_id] = {"count": 1, "timeout": 0}
                else:
                    failed_channels_pool[channel_id]["count"] += 1
                    
                if failed_channels_pool[channel_id]["count"] >= 2:
                    failed_channels_pool[channel_id]["timeout"] = time.time() + 1800
                    print(f"🚫 [DANH SÁCH ĐEN] Kênh {channel_id} lỗi liên tiếp 2 lần. Khóa quét kênh này trong 30 phút!", flush=True)
                continue 
            else:
                continue
        except Exception as e:
            continue

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
# 🧹 5. ĐÀO LẠI BÀI CŨ DỰA TRÊN VẾT CHECKPOINT (BẢN AN TOÀN TUYỆT ĐỐI)
# =====================================================================
async def follow_old_logic():
    global is_cleaning, channel_checkpoints
    if not auto_react_enabled: return

    is_cleaning = True
    print(f"🧹 [HỆ THỐNG] Tiến hành ĐÀO SÂU bài cũ...", flush=True)

    TARGET_PER_CHANNEL = 50
    MAX_LOOKBACK = 600
    global_temp_list = []

    shuffled_channels = TARGET_CHANNELS.copy()
    random.shuffle(shuffled_channels)

    try:
        for cid in shuffled_channels:
            if current_total_reacts >= TOTAL_REACT_LIMIT or not auto_react_enabled:
                break

            if cid in failed_channels_pool:
                if time.time() < failed_channels_pool[cid]["timeout"]:
                    continue
                else:
                    failed_channels_pool.pop(cid, None)

            channel = bot.get_channel(cid)
            if not channel: 
                print(f"⚠️ Không tìm thấy kênh {cid} (Thiếu quyền xem). Thử kênh khác...", flush=True)
                continue

            print(f"🔍 [QUÉT KÊNH] Đang kiểm tra lịch sử kênh: {cid}...", flush=True)
            channel_gathered = 0
            total_scanned = 0
            
            oldest_msg_id = channel_checkpoints.get(str(cid), {}).get("last_id")
            if oldest_msg_id:
                oldest_msg_id = int(oldest_msg_id)

            while channel_gathered < TARGET_PER_CHANNEL and total_scanned < MAX_LOOKBACK and auto_react_enabled:
                args = {"limit": 100} 
                if oldest_msg_id:
                    args["before"] = discord.Object(id=oldest_msg_id)

                history_chunk = []
                try:
                    async for msg in channel.history(**args): 
                        history_chunk.append(msg)
                except Exception as history_error: 
                    print(f"❌ Lỗi API khi lấy lịch sử kênh {cid}: {history_error}. Chuyển kênh!", flush=True)
                    break 

                if not history_chunk: 
                    print(f"ℹ️ Kênh {cid} đã bị đào cạn sạch lịch sử. Reset về đỉnh!", flush=True)
                    channel_checkpoints.pop(str(cid), None) 
                    break

                oldest_msg_id = history_chunk[-1].id
                total_scanned += len(history_chunk)

                # === BỘ LỌC PHÂN LOẠI ƯU TIÊN THEO USER ID ===
                for msg in history_chunk:
                    if msg.reactions:
                        my_reactions = [str(r.emoji) for r in msg.reactions if r.me]
                        missing_reactions = [r for r in msg.reactions if str(r.emoji) not in my_reactions]
                        
                        if missing_reactions:
                            # Sử dụng Tuple (msg, is_vip) để không ghi đè thuộc tính vào Class Message gốc
                            is_vip = msg.author.id in PRIORITY_USERS
                            global_temp_list.append((msg, is_vip))
                            
                            channel_gathered += 1
                            if channel_gathered >= TARGET_PER_CHANNEL: 
                                break
                del history_chunk

            if oldest_msg_id and auto_react_enabled:
                channel_checkpoints[str(cid)] = {"last_id": str(oldest_msg_id)}

        await save_all_data()

        # === SẮP XẾP HÀNG ĐỢI: NHÓM VIP ĐỨNG TRƯỚC ===
        if global_temp_list and auto_react_enabled:
            priority_list = [item[0] for item in global_temp_list if item[1] is True]
            normal_list = [item[0] for item in global_temp_list if item[1] is False]
            
            random.shuffle(priority_list)
            random.shuffle(normal_list)
            
            final_sorted_list = priority_list + normal_list
            print(f"🔄 Gom thành công {len(global_temp_list)} bài (Tìm thấy {len(priority_list)} bài từ VIP). Nạp hàng đợi...", flush=True)
            
            for msg in final_sorted_list:
                await reaction_queue.put(msg)
                
            del global_temp_list; del priority_list; del normal_list; del final_sorted_list
        else:
            if auto_react_enabled:
                print("ℹ️ Kết quả lượt quét: Không tìm thấy bài viết nào chứa emoji mới.", flush=True)

    except Exception as critical_error:
        print(f"🚨 [LỖI HỆ THỐNG] Đã xảy ra lỗi nghiêm trọng trong follow_old_logic: {critical_error}", flush=True)
    finally:
        is_cleaning = False

# =====================================================================
# 🔄 6. BỘ QUẢN LÝ VÒNG LẶP TUẦN TỰ (AN TOÀN TUYỆT ĐỐI)
# =====================================================================
async def auto_loop_manager():
    try:
        while True:
            if auto_react_enabled:
                print("\n🔄 [LOOP MANAGER] === BẮT ĐẦU MỘT CHU KỲ QUÉT MỎ MỚI ===", flush=True)
                start_reacts = current_total_reacts
                
                await follow_old_logic()
                
                if not reaction_queue.empty():
                    print("⏳ [HÀNG ĐỢI] Đang xử lý các bài viết trong hàng đợi...", flush=True)
                while not reaction_queue.empty() and auto_react_enabled:
                    await asyncio.sleep(1)
                
                reacts_gained = current_total_reacts - start_reacts
                
                if reacts_gained > 0:
                    print(f"⚡ [HỆ THỐNG] Lượt vừa rồi cày được {reacts_gained} react. Đào tiếp sau 15 giây...", flush=True)
                    await asyncio.sleep(15)
                else:
                    print("💤 Mỏ trống/Mỏ cạn. Hệ thống ngủ 30 giây trước khi đảo chu kỳ mới...", flush=True)
                    await asyncio.sleep(30)
            else:
                await asyncio.sleep(1)
    except asyncio.CancelledError:
        print("🔄 [LOOP MANAGER] Luồng quản lý vòng lặp cào bài tự động đã bị đóng hoàn toàn.", flush=True)

# --- 🔥 HỆ THỐNG LỆNH ĐIỀU KHIỂN START / STOP ---
@bot.command()
async def start(ctx):
    try: await ctx.message.delete()
    except: pass
    global auto_react_enabled, loop_manager_task, worker_task
    
    if auto_react_enabled:
        print("⚠️ Hệ thống Auto React vốn đã đang chạy rồi!", flush=True)
        return

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
    global auto_react_enabled, loop_manager_task, worker_task, is_cleaning
    
    if not auto_react_enabled:
        print("⚠️ Hệ thống hiện đang tắt, không cần dừng!", flush=True)
        return

    auto_react_enabled = False
    is_cleaning = False
    print("⛔ [DỪNG KHẨN CẤP] ĐANG KHAI TỬ TOÀN BỘ LUỒNG AUTO REACT VÀ WORKER...", flush=True)
    
    if loop_manager_task and not loop_manager_task.done():
        loop_manager_task.cancel()
    if worker_task and not worker_task.done():
        worker_task.cancel()
        
    loop_manager_task = None
    worker_task = None

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

@bot.event
async def on_ready():
    print(f"✅ Bot Online (Môi trường Railway Cloud) | Tiến độ hiện tại: {current_total_reacts}/{TOTAL_REACT_LIMIT}", flush=True)
    print(f"💡 Sử dụng lệnh !start để bắt đầu luồng cày và !stop để dừng khẩn cấp.", flush=True)

# =====================================================================
# 🚀 KHỞI CHẠY
# =====================================================================
keep_alive()

try:
    bot.run(TOKEN, bot=False, reconnect=True)
except Exception as e:
    print(f"❌ Lỗi kết nối Gateway: {e}", flush=True)
