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

# =====================================================================
# 🛠️ 1. KEEPALIVE SERVER (GIỮ BOT LUÔN THỨC TRÊN RAILWAY)
# =====================================================================
app = Flask('')
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route('/')
def home(): 
    return "OK", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, threaded=False, use_reloader=False)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()

keep_alive()

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
auto_react_enabled = True
reaction_queue = asyncio.Queue()
is_cleaning = False
channel_checkpoints = {}  # Lưu trữ checkpoint từng kênh vào file JSON

# Hệ thống cờ hiệu và bộ lọc cách ly kênh lỗi
trigger_next_clean = asyncio.Event()
failed_channels_pool = {}  

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
        "checkpoints": channel_checkpoints # Lưu kèm cả vết cào bài của từng kênh
    }
    await asyncio.to_thread(_sync_save_data, data)

# Khởi tạo nạp dữ liệu ban đầu từ ổ cứng Railway
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
# 📦 4. WORKER NGẦM XỬ LÝ HÀNG ĐỢI & KÍCH HOẠT QUAY VÒNG QUÉT
# =====================================================================
async def reaction_worker():
    while True:
        try:
            msg = await reaction_queue.get()
            while is_cleaning:
                await asyncio.sleep(0.5)

            if auto_react_enabled and current_total_reacts < TOTAL_REACT_LIMIT:
                await smart_react(msg, msg.channel.id)
                
        except Exception as e:
            print(f"❌ Lỗi Worker ngầm: {e}", flush=True)
        finally:
            reaction_queue.task_done()
            
            if reaction_queue.empty() and not is_cleaning and auto_react_enabled:
                print("🏁 [HÀNG ĐỢI TRỐNG] Đã xả hết sạch bài cũ! Đang kích hoạt vòng quét mới ngay lập tức...", flush=True)
                trigger_next_clean.set()

# =====================================================================
# 🧹 5. ĐÀO LẠI BÀI CŨ DỰA TRÊN VẾT CHECKPOINT (TỐI ƯU TỐC ĐỘ CÀO)
# =====================================================================
@bot.command(aliases=["clean"])
async def follow_old(ctx):
    global is_cleaning, channel_checkpoints
    try: await ctx.message.delete()
    except: pass
    if not auto_react_enabled: return

    is_cleaning = True
    print(f"🧹 [HỆ THỐNG] Đang sử dụng Checkpoint tiến hành ĐÀO SÂU bài cũ về quá khứ...", flush=True)

    TARGET_PER_CHANNEL = 35   # Lấy tối đa 35 bài chất lượng mỗi kênh trong lượt này
    MAX_LOOKBACK = 400        # Cho phép lội sâu tối đa 400 tin mỗi lượt để đào mỏ cũ
    global_temp_list = []

    shuffled_channels = TARGET_CHANNELS.copy()
    random.shuffle(shuffled_channels)

    for cid in shuffled_channels:
        if current_total_reacts >= TOTAL_REACT_LIMIT:
            break

        if cid in failed_channels_pool:
            if time.time() < failed_channels_pool[cid]["timeout"]:
                continue
            else:
                failed_channels_pool.pop(cid, None)

        channel = bot.get_channel(cid)
        if not channel: continue

        channel_gathered = 0
        total_scanned = 0
        
        # 🔥 ĐỌC VẾT CHECKPOINT CŨ CỦA KÊNH NÀY TỪ FILE
        oldest_msg_id = channel_checkpoints.get(str(cid), {}).get("last_id")
        if oldest_msg_id:
            oldest_msg_id = int(oldest_msg_id)

        while channel_gathered < TARGET_PER_CHANNEL and total_scanned < MAX_LOOKBACK:
            args = {"limit": 100} # Đẩy limit lên 100 tin mỗi lần cào để lội cực nhanh
            
            # Nếu đã có vết checkpoint, bot nhảy cóc thẳng xuống tin nhắn cũ đó để cào tiếp xuống
            if oldest_msg_id:
                args["before"] = discord.Object(id=oldest_msg_id)

            history_chunk = []
            try:
                async for msg in channel.history(**args): 
                    history_chunk.append(msg)
            except Exception as e: 
                print(f"⚠️ Không thể đọc lịch sử kênh {cid}: {e}", flush=True)
                break

            # Nếu không còn tin nhắn nào nữa (Đã đào cạn sạch lịch sử của kênh này)
            if not history_chunk: 
                print(f"ℹ️ Kênh {cid} đã bị đào cạn sạch lịch sử về quá khứ. Reset mốc quét về đỉnh!", flush=True)
                channel_checkpoints.pop(str(cid), None) # Xóa mốc cũ để lượt sau cào lại từ tin mới nhất
                break

            # Cập nhật điểm cũ nhất vừa chạm tới
            oldest_msg_id = history_chunk[-1].id
            total_scanned += len(history_chunk)

            for msg in history_chunk:
                if msg.reactions:
                    my_reactions = [str(r.emoji) for r in msg.reactions if r.me]
                    missing_reactions = [r for r in msg.reactions if str(r.emoji) not in my_reactions]
                    
                    if missing_reactions:
                        global_temp_list.append(msg)
                        channel_gathered += 1
                        if channel_gathered >= TARGET_PER_CHANNEL: 
                            break
            del history_chunk

        # 🔥 LƯU LẠI VẾT CHECKPOINT MỚI NHẤT VỪA ĐÀO ĐƯỢC CỦA KÊNH NÀY
        if oldest_msg_id:
            channel_checkpoints[str(cid)] = {"last_id": str(oldest_msg_id)}

    # Sau khi duyệt qua các kênh, tiến hành lưu lại toàn bộ mốc checkpoint lên ổ cứng Railway
    await save_all_data()

    if global_temp_list:
        print(f"🔄 Gom thành công {len(global_temp_list)} tin nhắn cũ hợp lệ từ mỏ checkpoint. Tiến hành trộn phẳng...", flush=True)
        random.shuffle(global_temp_list)
        random.shuffle(global_temp_list)

        for msg in global_temp_list:
            await reaction_queue.put(msg)
        print(f"📦 Đã phân bổ xong {len(global_temp_list)} tin vào hàng đợi xử lý tốc độ cao.", flush=True)
        del global_temp_list
    else:
        print("ℹ️ Mỏ cũ tạm thời chưa đào thêm được bài nào hợp lệ. Sẽ thử lại sau 20 giây...", flush=True)
        await asyncio.sleep(20)
        trigger_next_clean.set()

    is_cleaning = False

# =====================================================================
# 🔄 6. BỘ QUẢN LÝ VÒNG LẶP LIÊN TỤC THÔNG MINH AN TOÀN
# =====================================================================
async def auto_loop_manager():
    await bot.wait_until_ready()
    
    class FakeContext:
        async def delete(self): pass
    ctx = FakeContext()
    
    while True:
        try:
            if auto_react_enabled:
                start_reacts = current_total_reacts
                
                await follow_old(ctx)
                
                await trigger_next_clean.wait()
                trigger_next_clean.clear() 
                
                reacts_gained = current_total_reacts - start_reacts
                
                if reacts_gained > 0:
                    print(f"⚡ [HỆ THỐNG] Lượt vừa rồi cày được {reacts_gained} react. Đào tiếp vết cũ sau 5s...", flush=True)
                    await asyncio.sleep(5)
                else:
                    print("⚠️ [THÔNG BÁO] Lượt đào này không phát sinh thêm react nào mới.", flush=True)
                    print("💤 Hệ thống nghỉ 30 giây để giãn cách an toàn API trước khi đào sâu tiếp...", flush=True)
                    await asyncio.sleep(30)
            else:
                await asyncio.sleep(5)
                
        except Exception as e:
            print(f"❌ Lỗi luồng quản lý vòng lặp: {e}", flush=True)
            await asyncio.sleep(15)

# --- CÁC LỆNH ĐIỀU KHIỂN BỔ TRỢ ---
@bot.command()
async def total(ctx, num: int):
    global TOTAL_REACT_LIMIT
    TOTAL_REACT_LIMIT = num
    await save_all_data()
    print(f"♻️ Hạn mức mới: {num}", flush=True)

@bot.command()
async def start(ctx):
    global auto_react_enabled
    auto_react_enabled = True
    trigger_next_clean.set()
    print("▶️ BẬT AUTO REACT", flush=True)

@bot.command()
async def stop(ctx):
    global auto_react_enabled
    auto_react_enabled = False
    await save_all_data()
    print("⛔ DỪNG AUTO REACT", flush=True)

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
    bot.loop.create_task(reaction_worker())
    bot.loop.create_task(auto_loop_manager())
    print(f"✅ Bot Online (Môi trường Railway Cloud) | Tiến độ: {current_total_reacts}/{TOTAL_REACT_LIMIT}", flush=True)

try:
    bot.run(TOKEN, bot=False, reconnect=True)
except Exception as e:
    print(f"❌ Lỗi kết nối Gateway: {e}", flush=True)
