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

# Tắt log hiển thị phiền phức của Flask để console sạch sẽ
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route('/')
def home():
    return "OK", 200

def run_flask():
    # Railway tự động cấp biến môi trường PORT, nếu không có sẽ dùng 10000
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, threaded=False, use_reloader=False)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()

keep_alive()

# =====================================================================
# ⚙️ 2. CẤU HÌNH ĐƯỜNG DẪN Ổ CỨNG (DÀNH RIÊNG CHO RAILWAY VOLUME)
# =====================================================================
TOKEN = os.getenv("DISCORD_TOKEN")
prefix = "!"

# Đường dẫn chuẩn trỏ vào Volume đã mount trên Railway để lưu checkpoint vĩnh viễn
checkpoint_file = "/app/data/checkpoints_multi.json"
channels_file = "/app/data/channels.txt"
backup_file = "backup_channels.txt"  # File dự phòng nằm trong gói code GitHub

intents = discord.Intents.all()
bot = commands.Bot(command_prefix=prefix,
                   help_command=None,
                   intents=intents,
                   self_bot=True)

TOTAL_REACT_LIMIT = 15000
current_total_reacts = 0
auto_react_enabled = True
reaction_queue = asyncio.Queue()
is_cleaning = False

# --- HÀM QUẢN LÝ DỮ LIỆU ĐƯỢC ỦY QUYỀN LUỒNG (CHỐNG NGHẼN Ổ CỨNG) ---
def _sync_load_data():
    default_data = {"stats": {"current_total": 0, "limit": TOTAL_REACT_LIMIT}}
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "stats" not in data: data["stats"] = default_data["stats"]
                return data
        except:
            return default_data
    return default_data

def _sync_save_data(data):
    try:
        # Đảm bảo thư mục /app/data/ tồn tại trước khi ghi
        os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"❌ Lỗi ghi file checkpoint trên Railway Volume: {e}")

def _sync_load_channels():
    # Nếu file channels.txt trong Volume chưa tồn tại, tạo mới trống
    os.makedirs(os.path.dirname(channels_file), exist_ok=True)
    if not os.path.exists(channels_file):
        with open(channels_file, "w", encoding="utf-8") as f: pass

    # Đọc thử danh sách kênh hiện tại trong Volume
    with open(channels_file, "r", encoding="utf-8") as f:
        channels = [int(line.strip()) for line in f if line.strip() and not line.startswith("#")]

    # NẾU VOLUME TRỐNG -> Lấy danh sách từ file dự phòng GitHub đè vào Volume
    if not channels and os.path.exists(backup_file):
        print("💡 [HỆ THỐNG] Phát hiện Volume trống. Đang nạp kênh từ backup_channels.txt của GitHub...")
        with open(backup_file, "r", encoding="utf-8") as f_backup:
            backup_content = f_backup.read()
        
        with open(channels_file, "w", encoding="utf-8") as f_volume:
            f_volume.write(backup_content)
        
        channels = [int(line.strip()) for line in backup_content.split("\n") if line.strip() and not line.startswith("#")]

    return channels

async def save_all_data():
    data = {
        "stats": {
            "current_total": current_total_reacts,
            "limit": TOTAL_REACT_LIMIT
        }
    }
    # Đẩy việc ghi file ra luồng riêng (Thread) để không làm nghẽn Event Loop của Discord khi Railway lag ổ cứng
    await asyncio.to_thread(_sync_save_data, data)

# Nạp dữ liệu ban đầu
data_store = _sync_load_data()
current_total_reacts = data_store["stats"]["current_total"]
TOTAL_REACT_LIMIT = data_store["stats"]["limit"]
TARGET_CHANNELS = _sync_load_channels()

# =====================================================================
# ⚡ HÀM REACT SIÊU TỐC (ÉP TIẾN ĐỘ CHẠY ĐỦ NHANH)
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
            
            # 🔥 ĐẨY TỐC ĐỘ LÊN CỰC ĐẠI: Chỉ nghỉ từ 0.25s đến 0.5s giữa các quả emoji
            # Tốc độ này tương đương ~150 - 200 react / phút, cực nhanh nhưng không bị gậy Rate Limit tổng
            await asyncio.sleep(random.uniform(0.25, 0.5)) 
        except Exception as e:
            print(f"⚠️ Lỗi thả emoji tại kênh {channel_id}: {e}", flush=True)
            break

    # Cứ cày được 30 quả thì lưu checkpoint một lần để tránh nghẽn ổ cứng
    if current_total_reacts % 30 == 0:
        await save_all_data()

# =====================================================================
# 📦 WORKER XỬ LÝ HÀNG ĐỢI (XÓA BỎ LỆNH NGỦ THỪA CHỐNG NGHẼN)
# =====================================================================
async def reaction_worker():
    while True:
        try:
            msg = await reaction_queue.get()
            while is_cleaning:
                await asyncio.sleep(1)

            if auto_react_enabled and current_total_reacts < TOTAL_REACT_LIMIT:
                # Gọi hàm thả react
                await smart_react(msg, msg.channel.id)
                
                # 🔥 ĐÃ XÓA LỆNH NGỦ THỪA Ở ĐÂY! 
                # Bot sẽ lập tức bốc tin nhắn tiếp theo trong hàng đợi ra xử lý luôn không cần đợi tiếp.
                
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
        await asyncio.sleep(random.uniform(5, 8))
        try:
            refreshed_msg = await m.channel.fetch_message(m.id)
            if refreshed_msg.reactions:
                await reaction_queue.put(refreshed_msg)
        except:
            pass

    bot.loop.create_task(wait_and_push(message))
    await bot.process_commands(message)

# =====================================================================
# 🧹 LỆNH CÀO BÀI THAM LAM (GOM ÍT NHẤT 200 - 500 TIN MỖI LƯỢT CHẠY)
# =====================================================================
@bot.command(aliases=["clean"])
async def follow_old(ctx):
    global is_cleaning
    try: await ctx.message.delete()
    except: pass
    if not auto_react_enabled: return

    is_cleaning = True
    print(f"🧹 [HỆ THỐNG] Đang cào bài tham lam diện rộng (Mục tiêu >200 tin)...", flush=True)

    # 🔥 CẤU HÌNH THAM LAM:
    TARGET_PER_CHANNEL = 35   # Lấy tối đa 35 tin có emoji trên MỖI KÊNH
    MAX_LOOKBACK = 250        # Lội ngược dòng sâu tối đa 250 tin mỗi kênh để tìm bài
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
            
            # 🔥 THAY ĐỔI QUAN TRỌNG: 
            # Nếu là vòng lặp tự động (ctx không có tin nhắn thật), ta BỎ QUA last_id của checkpoint cũ.
            # Điều này ép bot luôn luôn quét từ các bài MỚI NHẤT hiện tại trở xuống để nhặt được nhiều tin nhất.
            if oldest_msg_id:
                args["before"] = discord.Object(id=oldest_msg_id)
            elif ctx and hasattr(ctx, 'message') and ctx.message:
                # Nếu bạn gõ lệnh gõ tay !clean, bot vẫn có thể dùng checkpoint nếu muốn
                last_id = channel_checkpoints.get(str(cid), {}).get("last_id")
                if last_id: args["before"] = discord.Object(id=int(last_id))

            history_chunk = []
            try:
                async for msg in channel.history(**args):
                    history_chunk.append(msg)
            except Exception as e:
                print(f"❌ Lỗi đọc lịch sử kênh {cid}: {e}", flush=True)
                break

            if not history_chunk:
                break

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

        # Ghi nhận điểm dừng cho lệnh gõ tay
        if oldest_msg_id:
            channel_checkpoints[str(cid)] = {"last_id": str(oldest_msg_id)}

    # Trộn phẳng ngẫu nhiên để các kênh xen kẽ nhau (Phá cụm Rate Limit cục bộ của Discord)
    if global_temp_list:
        print(f"🔄 Gom thành công {len(global_temp_list)} tin nhắn. Tiến hành trộn phẳng...", flush=True)
        random.shuffle(global_temp_list)
        random.shuffle(global_temp_list)

        for msg in global_temp_list:
            await reaction_queue.put(msg)
        
        print(f"📦 Đã phân bổ xong {len(global_temp_list)} tin vào hàng đợi xử lý tốc độ cao.", flush=True)
        del global_temp_list

    is_cleaning = False
    print(f"🏁 [HỆ THỐNG] Sẵn sàng cày cuốc!", flush=True)

# =====================================================================
# ⏰ 5. VÒNG LẶP ĐỒNG HỒ TỰ ĐỘNG CÀO BÀI (ÉP CHẠY XUYÊN ĐÊM KHI TẮT MÁY)
# =====================================================================
async def auto_clean_loop():
    await bot.wait_until_ready()
    while True:
        try:
            # Tự động gọi lệnh quét sau mỗi 45 phút nếu hệ thống không bận
            if auto_react_enabled and not is_cleaning:
                print("⏰ [ĐỒNG HỒ] Đến giờ hẹn! Bot tự động kích hoạt cào bài mới bất chấp tắt máy nhà...")
                class FakeContext:
                    async def delete(self): pass
                ctx = FakeContext()
                await follow_old(ctx)
        except Exception as e:
            print(f"❌ Lỗi vòng lặp tự động: {e}")
        
        await asyncio.sleep(2700) # 45 phút chạy một lần

# --- CÁC LỆNH ĐIỀU KHIỂN BỔ TRỢ ---
@bot.command()
async def total(ctx, num: int):
    global TOTAL_REACT_LIMIT
    TOTAL_REACT_LIMIT = num
    await save_all_data()
    try: await ctx.message.delete()
    except: pass
    print(f"♻️ Hạn mức mới: {num}")

@bot.command()
async def get_backup(ctx):
    try:
        try: await ctx.message.delete()
        except: pass
        if os.path.exists(checkpoint_file):
            await ctx.send("📦 Đây là file checkpoint mới nhất lưu tại Railway Volume:", 
                           file=discord.File(checkpoint_file))
            print("✅ Đã xuất file checkpoint qua Discord!")
        else:
            await ctx.send("❌ Không tìm thấy file checkpoint trên Volume!")
    except Exception as e:
        print(f"❌ Lỗi trích xuất file: {e}")

@bot.command()
async def reload(ctx):
    global TARGET_CHANNELS
    try:
        TARGET_CHANNELS = await asyncio.to_thread(_sync_load_channels)
        try: await ctx.message.delete()
        except: pass
        print(f"🔄 ĐÃ CẬP NHẬT: Hiện có {len(TARGET_CHANNELS)} kênh.")
    except Exception as e:
        print(f"❌ Lỗi reload danh sách kênh: {e}")

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
    bot.loop.create_task(auto_clean_loop())
    print(f"✅ Bot Online (Môi trường Railway Cloud) | Tiến độ lưu ổ cứng: {current_total_reacts}/{TOTAL_REACT_LIMIT}")

# --- KÍCH HOẠT VÀ ÉP GIỮ PHIÊN KẾT NỐI KHI TẮT MÁY ---
try:
    bot.run(TOKEN, bot=False, reconnect=True)
except Exception as e:
    print(f"❌ Lỗi kết nối Gateway: {e}")
