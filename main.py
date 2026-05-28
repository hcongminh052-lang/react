import json
import os
import asyncio
import random
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
def home(): return "OK", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, threaded=False, use_reloader=False)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()

keep_alive()

# =====================================================================
# ⚙️ 2. CẤU HÌNH BIẾN TOÀN CỤC VÀ HỆ THỐNG CỜ HIỆU VÒNG LẶP
# =====================================================================
TOKEN = os.getenv("DISCORD_TOKEN")
prefix = "!"

checkpoint_file = "/app/data/checkpoints_multi.json"
channels_file = "/app/data/channels.txt"
backup_file = "backup_channels.txt"

intents = discord.Intents.all()
bot = commands.Bot(command_prefix=prefix, help_command=None, intents=intents, self_bot=True)

TOTAL_REACT_LIMIT = 50000
current_total_reacts = 0
auto_react_enabled = True
reaction_queue = asyncio.Queue()
is_cleaning = False
channel_checkpoints = {}

# 🔥 CỜ HIỆU THÔNG MINH: Dùng để ra lệnh cho bot đi cào bài mới ngay lập tức
trigger_next_clean = asyncio.Event()

def _sync_load_data():
    default_data = {"stats": {"current_total": 0, "limit": TOTAL_REACT_LIMIT}}
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "stats" not in data: data["stats"] = default_data["stats"]
                return data
        except: return default_data
    return default_data

def _sync_save_data(data):
    try:
        os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e: print(f"❌ Lỗi ghi file checkpoint: {e}", flush=True)

def _sync_load_channels():
    os.makedirs(os.path.dirname(channels_file), exist_ok=True)
    if not os.path.exists(channels_file):
        with open(channels_file, "w", encoding="utf-8") as f: pass
    with open(channels_file, "r", encoding="utf-8") as f:
        channels = [int(line.strip()) for line in f if line.strip() and not line.startswith("#")]
    if not channels and os.path.exists(backup_file):
        with open(backup_file, "r", encoding="utf-8") as f_backup: backup_content = f_backup.read()
        with open(channels_file, "w", encoding="utf-8") as f_volume: f_volume.write(backup_content)
        channels = [int(line.strip()) for line in backup_content.split("\n") if line.strip() and not line.startswith("#")]
    return channels

async def save_all_data():
    data = {"stats": {"current_total": current_total_reacts, "limit": TOTAL_REACT_LIMIT}}
    await asyncio.to_thread(_sync_save_data, data)

data_store = _sync_load_data()
current_total_reacts = data_store["stats"]["current_total"]
TOTAL_REACT_LIMIT = data_store["stats"]["limit"]
TARGET_CHANNELS = _sync_load_channels()

# =====================================================================
# ⚡ 3. HÀM THẢ REACT TỐC ĐỘ CAO (AN TOÀN + BỎ QUA EMOJI LỖI 10014)
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
            await asyncio.sleep(random.uniform(0.25, 0.5)) # Tốc độ cực nhanh cực mượt
        except discord.errors.HTTPException as e:
            if e.code == 10014:
                print(f"⚠️ Bỏ qua emoji lỗi 10014 tại kênh {channel_id}", flush=True)
                continue # Nhảy sang quả tiếp theo, không phá luồng
            else:
                continue
        except Exception as e:
            continue

    if current_total_reacts % 30 == 0:
        await save_all_data()

# =====================================================================
# 📦 4. WORKER NGẦM & BẮN TÍN HIỆU LẶP LẠI NGAY KHI HẾT BÀI
# =====================================================================
async def reaction_worker():
    while True:
        try:
            # Lấy tin nhắn từ hàng đợi ra xử lý
            msg = await reaction_queue.get()
            while is_cleaning:
                await asyncio.sleep(0.5)

            if auto_react_enabled and current_total_reacts < TOTAL_REACT_LIMIT:
                await smart_react(msg, msg.channel.id)
                
        except Exception as e:
            print(f"❌ Lỗi Worker ngầm: {e}", flush=True)
        finally:
            reaction_queue.task_done()
            
            # 🔥 ĐÂY LÀ KHÚC PHÁT TÍN HIỆU LẶP LẠI:
            # Nếu hàng đợi trống rỗng hoàn toàn, kích hoạt cờ hiệu để ép bot đi cào bài tiếp ngay lập tức!
            if reaction_queue.empty() and not is_cleaning and auto_react_enabled:
                print("🏁 [HÀNG ĐỢI TRỐNG] Đã xả hết sạch bài cũ! Đang kích hoạt vòng quét mới ngay lập tức...", flush=True)
                trigger_next_clean.set()

# =====================================================================
# 🧹 5. CÀO BÀI DIỆN RỘNG (MỤC TIÊU LỚN KHÔNG DÙNG CHECKPOINT CŨ)
# =====================================================================
@bot.command(aliases=["clean"])
async def follow_old(ctx):
    global is_cleaning
    try: await ctx.message.delete()
    except: pass
    if not auto_react_enabled: return

    is_cleaning = True
    print(f"🧹 [HỆ THỐNG] Đang tiến hành cào bài tham lam diện rộng...", flush=True)

    TARGET_PER_CHANNEL = 35   # Lấy tối đa 35 bài có emoji mỗi kênh
    MAX_LOOKBACK = 250        # Lội ngược sâu tối đa 250 bài mỗi kênh để tìm tin nhắn chất lượng
    global_temp_list = []

    shuffled_channels = TARGET_CHANNELS.copy()
    random.shuffle(shuffled_channels)

    for cid in shuffled_channels:
        if current_total_reacts >= TOTAL_REACT_LIMIT:
            break

        channel = bot.get_channel(cid)
        if not channel: continue

        channel_gathered = 0
        total_scanned = 0
        oldest_msg_id = None

        while channel_gathered < TARGET_PER_CHANNEL and total_scanned < MAX_LOOKBACK:
            args = {"limit": 50}
            
            # Bỏ qua checkpoint khi chạy tự động để luôn lội ngược dòng từ tin mới nhất trở xuống
            if oldest_msg_id:
                args["before"] = discord.Object(id=oldest_msg_id)

            history_chunk = []
            try:
                async for msg in channel.history(**args): history_chunk.append(msg)
            except: break

            if not history_chunk: break

            oldest_msg_id = history_chunk[-1].id
            total_scanned += len(history_chunk)

            for msg in history_chunk:
                if msg.reactions:
                    my_reactions = [str(r.emoji) for r in msg.reactions if r.me]
                    missing_reactions = [r for r in msg.reactions if str(r.emoji) not in my_reactions]
                    
                    if missing_reactions:
                        global_temp_list.append(msg)
                        channel_gathered += 1
                        if channel_gathered >= TARGET_PER_CHANNEL: break
            del history_chunk

    if global_temp_list:
        print(f"🔄 Gom thành công {len(global_temp_list)} tin nhắn hợp lệ. Tiến hành trộn phẳng...", flush=True)
        random.shuffle(global_temp_list)
        random.shuffle(global_temp_list)

        for msg in global_temp_list:
            await reaction_queue.put(msg)
        print(f"📦 Đã phân bổ xong {len(global_temp_list)} tin vào hàng đợi xử lý tốc độ cao.", flush=True)
        del global_temp_list
    else:
        # Nếu xui xẻo không gom được tin nào, cho nghỉ 30 giây rồi tự kích hoạt lại để tránh thắt nút cổ chai
        print("ℹ️ Không tìm thấy tin nhắn mới nào phù hợp. Sẽ thử lại sau 30 giây...", flush=True)
        await asyncio.sleep(30)
        trigger_next_clean.set()

    is_cleaning = False

# =====================================================================
# 🔄 6. QUẢN LÝ VÒNG LẶP VÔ HẠN THEO SỰ KIỆN (THAY CHO ĐỒNG HỒ 45 PHÚT)
# =====================================================================
async def auto_loop_manager():
    await bot.wait_until_ready()
    
    # Lần đầu tiên mở bot, tự động kích hoạt lượt quét khai mạc luôn
    class FakeContext:
        async def delete(self): pass
    ctx = FakeContext()
    
    while True:
        try:
            if auto_react_enabled:
                await follow_old(ctx)
            
            # 🛑 Đứng đợi ở đây: Khi nào Worker thả hết sạch bài và gọi trigger_next_clean.set(),
            # thì dòng lệnh phía dưới mới được chạy tiếp, tạo thành một vòng lặp liên tục không kẽ hở.
            await trigger_next_clean.wait()
            trigger_next_clean.clear() # Reset cờ hiệu về trạng thái chờ cho lượt sau
            
            # Nghỉ chân 5 giây ngắn ngủi giữa các lượt cào lớn để bảo vệ bot chống quá tải RAM trên Cloud
            await asyncio.sleep(5)
            
        except Exception as e:
            print(f"❌ Lỗi luồng quản lý vòng lặp: {e}", flush=True)
            await asyncio.sleep(10)

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
    trigger_next_clean.set() # Bật lại là ép cào luôn
    print("▶️ BẬT AUTO REACT", flush=True)

@bot.command()
async def stop(ctx):
    global auto_react_enabled
    auto_react_enabled = False
    await save_all_data()
    print("⛔ DỪNG AUTO REACT", flush=True)

@bot.event
async def on_ready():
    bot.loop.create_task(reaction_worker())
    bot.loop.create_task(auto_loop_manager()) # Kích hoạt bộ quản lý vòng lặp theo sự kiện
    print(f"✅ Bot Online (Môi trường Railway Cloud) | Tiến độ: {current_total_reacts}/{TOTAL_REACT_LIMIT}", flush=True)

try:
    bot.run(TOKEN, bot=False, reconnect=True)
except Exception as e:
    print(f"❌ Lỗi kết nối Gateway: {e}", flush=True)
