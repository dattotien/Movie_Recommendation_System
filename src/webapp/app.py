# src/webapp/app.py
import sys
import os
from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import requests

# --- Thêm đường dẫn để import ---
# Thêm 'src' (để import utils)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Thêm thư mục gốc (để import scripts)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# --- Import các file của bạn ---
# (Đảm bảo file này là file tôi đã gửi, có hàm get_cassandra_session, read_recs)
from utils.cassandra_connector import get_cassandra_session, read_recs, create_keyspace_and_table
create_keyspace_and_table()
# (Đảm bảo file này là file tôi đã gửi, có hàm send_new_rating)
from scripts.kafka_producer import send_new_rating

app = Flask(__name__)
app.secret_key = 'bigdata_secret_key' # Cần cho flash messages

# === CẤU HÌNH API VÀ BỘ ĐỆM (CACHE) ===
TMDB_API_KEY = "f725eef7825ef832dc7836cdf97fb091" # <<<<< THAY KEY CỦA BẠN VÀO ĐÂY
TMDB_POSTER_BASE_URL = "https://image.tmdb.org/t/p/w200"
TMDB_POSTER_BASE_URL_W500 = "https://image.tmdb.org/t/p/w500"
POSTER_CACHE = {} # Cache đơn giản để lưu poster
MOVIE_MAP = {} # Map chi tiết phim
MOVIE_DETAILS_CACHE = {}
def get_rich_movie_details(tmdb_id):
    """
    Lấy chi tiết đầy đủ (mô tả, ngày phát hành...) từ tmdbId (có cache).
    """
    if not tmdb_id or pd.isna(tmdb_id):
        return None

    if tmdb_id in MOVIE_DETAILS_CACHE:
        return MOVIE_DETAILS_CACHE[tmdb_id]
        
    try:
        # Thêm language=vi-VN để lấy mô tả tiếng Việt (nếu có)
        api_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=vi-VN"
        response = requests.get(api_url, timeout=5)
        response.raise_for_status()
        
        data = response.json()
        
        # Lấy các thông tin cần thiết
        details = {
            "overview": data.get('overview', 'Không có mô tả.'),
            "release_date": data.get('release_date', 'Không rõ'),
            "tagline": data.get('tagline', ''),
            # Lấy poster lớn hơn cho trang chi tiết
            "poster_url_large": f"{TMDB_POSTER_BASE_URL_W500}{data.get('poster_path')}" if data.get('poster_path') else None
        }
        
        MOVIE_DETAILS_CACHE[tmdb_id] = details
        return details
        
    except Exception as e:
        print(f"Lỗi khi gọi API TMDb chi tiết cho ID {tmdb_id}: {e}", file=sys.stderr)
        MOVIE_DETAILS_CACHE[tmdb_id] = None # Cache lỗi để không gọi lại
        return None
@app.route('/movie/<int:movie_id>')
def movie_details(movie_id):
    movie_info = None
    rich_details = None
    error = None
    
    # Lấy User ID từ URL (sẽ được truyền từ link ở index.html)
    user_id = request.args.get('user_id', '')

    try:
        # 1. Lấy thông tin cơ bản từ MOVIE_MAP
        movie_info = MOVIE_MAP.get(movie_id)
        
        if movie_info:
            # 2. Lấy thông tin chi tiết từ TMDb
            tmdb_id = movie_info.get('tmdbId')
            rich_details = get_rich_movie_details(tmdb_id)
            
            # Gộp thông tin lại
            movie_info['id'] = movie_id # Đảm bảo có ID
            movie_info.update(rich_details if rich_details else {})
            
            # Xử lý nếu không có poster lớn
            if 'poster_url_large' not in movie_info or not movie_info['poster_url_large']:
                 movie_info['poster_url_large'] = get_poster_url(tmdb_id) # Dùng lại poster nhỏ

        else:
            error = f"Không tìm thấy phim với ID: {movie_id}"

    except Exception as e:
        error = f"Lỗi khi tải chi tiết phim: {e}"
        print(error, file=sys.stderr)

    return render_template('movie_details.html', 
                           movie=movie_info, 
                           user_id_to_rate=user_id,
                           error=error)
def load_movie_details(movies_csv_path, links_csv_path):
    """
    Hàm mới: Đọc movies.csv và links.csv, gộp chúng lại.
    Tạo ra một map: { movieId -> {"title": "...", "tmdbId": "..."} }
    """
    try:
        movies_df = pd.read_csv(movies_csv_path)
        links_df = pd.read_csv(links_csv_path, dtype={'tmdbId': str}) # Đọc tmdbId dạng chuỗi
        
        movie_details_df = pd.merge(movies_df, links_df, on='movieId')
        print(f"Đã tải và gộp {len(movie_details_df)} chi tiết phim.")
        
        details_map = {}
        for _, row in movie_details_df.iterrows():
            details_map[row['movieId']] = {
                'title': row['title'],
                'genres': row['genres'],
                'tmdbId': row.get('tmdbId')
            }
        return details_map
    except Exception as e:
        print(f"Lỗi nghiêm trọng khi tải chi tiết phim: {e}", file=sys.stderr)
        return {}

def get_poster_url(tmdb_id):
    """
    Lấy poster URL từ tmdbId (có cache).
    """
    if not tmdb_id or pd.isna(tmdb_id):
        return None

    if tmdb_id in POSTER_CACHE:
        return POSTER_CACHE[tmdb_id]
        
    try:
        api_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}"
        response = requests.get(api_url, timeout=5) # Thêm timeout 5s
        response.raise_for_status()
        
        data = response.json()
        poster_path = data.get('poster_path')
        
        if poster_path:
            full_url = f"{TMDB_POSTER_BASE_URL}{poster_path}"
            POSTER_CACHE[tmdb_id] = full_url
            return full_url
        else:
            POSTER_CACHE[tmdb_id] = None
            return None
    except Exception as e:
        print(f"Lỗi khi gọi API TMDb cho ID {tmdb_id}: {e}", file=sys.stderr)
        POSTER_CACHE[tmdb_id] = None
        return None

def get_fallback_image(tmdb_id, title):
    """
    Tìm ảnh thay thế nếu không có poster.
    Thử lấy backdrop hoặc dùng placeholder có tên phim.
    """
    if not tmdb_id or pd.isna(tmdb_id):
        # Dùng placeholder với tên phim
        title_encoded = requests.utils.quote(title[:30]) if title else "No+Image"
        return f'https://via.placeholder.com/200x300.png?text={title_encoded}'
    
    try:
        # Thử lấy backdrop image từ TMDB
        api_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}"
        response = requests.get(api_url, timeout=5)
        response.raise_for_status()
        
        data = response.json()
        backdrop_path = data.get('backdrop_path')
        
        if backdrop_path:
            # Dùng backdrop image (ảnh nền) làm thay thế
            backdrop_url = f"https://image.tmdb.org/t/p/w300{backdrop_path}"
            return backdrop_url
        else:
            # Không có backdrop, dùng placeholder với tên phim
            title_encoded = requests.utils.quote(title[:30]) if title else "No+Image"
            return f'https://via.placeholder.com/200x300.png?text={title_encoded}'
    except Exception as e:
        # Lỗi API, dùng placeholder với tên phim
        title_encoded = requests.utils.quote(title[:30]) if title else "No+Image"
        return f'https://via.placeholder.com/200x300.png?text={title_encoded}'

def get_poster_from_other_movie(exclude_movie_ids):
    """
    Tìm poster từ một phim khác trong MOVIE_MAP (không nằm trong danh sách exclude_movie_ids).
    """
    import random
    
    # Lấy danh sách tất cả movie IDs trong MOVIE_MAP
    all_movie_ids = list(MOVIE_MAP.keys())
    
    # Loại bỏ các phim đã có trong recommendations
    available_movie_ids = [mid for mid in all_movie_ids if mid not in exclude_movie_ids]
    
    # Thử tìm poster từ các phim khác (random hoặc theo thứ tự)
    random.shuffle(available_movie_ids)
    
    for other_movie_id in available_movie_ids[:50]:  # Thử tối đa 50 phim
        other_movie = MOVIE_MAP.get(other_movie_id)
        if other_movie:
            other_tmdb_id = other_movie.get('tmdbId')
            if other_tmdb_id:
                poster_url = get_poster_url(other_tmdb_id)
                if poster_url and 'via.placeholder.com' not in poster_url:
                    return poster_url
    
    # Nếu không tìm thấy, trả về None
    return None

# === KHỞI ĐỘNG APP ===
# 1. Khởi tạo kết nối Cassandra (hàm này sẽ tự tạo keyspace/table)
print("Initializing Cassandra Session...")
get_cassandra_session() 

# 2. Tải chi tiết phim
print("Loading movie details map...")
MOVIE_MAP_PATH = 'data/ml-32m/ml-32m/movies.csv'
LINKS_MAP_PATH = 'data/ml-32m/ml-32m/links.csv'
MOVIE_MAP = load_movie_details(MOVIE_MAP_PATH, LINKS_MAP_PATH)
print("Web App is ready.")

# === ROUTE 1: TRANG CHỦ ===
@app.route('/', methods=['GET'])
def index():
    # Chỉ hiển thị trang, dùng template 'index_bigdata.html'
    return render_template('index.html')

# === ROUTE 2: LẤY GỢI Ý (ĐỌC TỪ CASSANDRA) ===
@app.route('/get_recs', methods=['POST'])
def get_recommendations():
    user_id = request.form.get('user_id_lookup')
    recommendations = []
    error = None
    
    if not user_id:
        error = "Vui lòng nhập User ID để tra cứu."
    else:
        try:
            # === THAY ĐỔI CHÍNH Ở ĐÂY ===
            # Thay vì đọc từ C*, ta giả lập 1 danh sách ID phim
            movie_id_list = read_recs(user_id) # << DÒNG GỐC
            # movie_id_list = [1, 2, 3, 4, 5, 110, 260, 593] # << DÒNG THAY THẾ
            # (Bạn có thể dùng bất kỳ ID nào có trong file movies.csv)
            # ============================
            
            if movie_id_list:
                # Chỉ lấy phim có tên (bắt buộc)
                movies_with_title = []  # Chỉ lưu phim có tên
                exclude_ids = set(int(mid) for mid in movie_id_list)  # Danh sách ID cần loại trừ khi tìm poster
                
                # Bước 1: Thu thập tất cả phim có tên
                for movie_id in movie_id_list:
                    movie_details = MOVIE_MAP.get(int(movie_id))
                    
                    if movie_details:
                        title = movie_details.get('title', '')
                        # Chỉ lấy phim có tên (bắt buộc)
                        if not title or title.startswith('Không rõ tên'):
                            continue  # Bỏ qua phim không có tên
                        
                        tmdb_id = movie_details.get('tmdbId')
                        poster_url = get_poster_url(tmdb_id)
                        
                        # Nếu không có poster, thử tìm ảnh thay thế
                        if not poster_url:
                            poster_url = get_fallback_image(tmdb_id, title)
                        
                        # Kiểm tra có poster thật (không phải placeholder)
                        has_real_poster = (poster_url and 
                                         poster_url != 'https://via.placeholder.com/200x300.png?text=No+Image' and
                                         'via.placeholder.com' not in poster_url)
                        
                        movies_with_title.append({
                            "id": movie_id,
                            "title": title,
                            "poster_url": poster_url,
                            "has_real_poster": has_real_poster
                        })
                
                # Bước 2: Thay thế poster cho phim không có poster bằng poster của phim KHÁC (không trong danh sách)
                for movie in movies_with_title:
                    if not movie["has_real_poster"]:
                        # Tìm poster từ một phim khác trong MOVIE_MAP (không nằm trong recommendations)
                        replacement_poster = get_poster_from_other_movie(exclude_ids)
                        
                        if replacement_poster:
                            movie["poster_url"] = replacement_poster
                        else:
                            # Nếu không tìm thấy, dùng placeholder
                            movie["poster_url"] = 'https://via.placeholder.com/200x300.png?text=No+Image'
                
                # Bước 3: Phân loại theo ưu tiên (có poster thật vs poster thay thế)
                complete_recs = []  # Có poster thật + tên
                incomplete_recs = []  # Chỉ có tên (poster đã được thay thế)
                
                for movie in movies_with_title:
                    # Kiểm tra lại poster sau khi thay thế
                    has_real_poster = (movie["poster_url"] and 
                                     movie["poster_url"] != 'https://via.placeholder.com/200x300.png?text=No+Image' and
                                     'via.placeholder.com' not in movie["poster_url"])
                    
                    if has_real_poster:
                        complete_recs.append({
                            "id": movie["id"],
                            "title": movie["title"],
                            "poster_url": movie["poster_url"]
                        })
                    else:
                        incomplete_recs.append({
                            "id": movie["id"],
                            "title": movie["title"],
                            "poster_url": movie["poster_url"]
                        })
                
                # Bước 4: Lấy 10 phim theo thứ tự ưu tiên
                recommendations = complete_recs[:10]
                if len(recommendations) < 10:
                    needed = 10 - len(recommendations)
                    recommendations.extend(incomplete_recs[:needed])
                
                # Debug log
                if len(recommendations) < 10:
                    print(f"⚠️  Chỉ tìm được {len(recommendations)} phim có tên từ top {len(movie_id_list)} recommendations", file=sys.stderr)
                    print(f"   - Có poster thật: {len(complete_recs)}, Có poster thay thế: {len(incomplete_recs)}", file=sys.stderr)
            else:
                error = f"Không tìm thấy gợi ý cho User {user_id} (Lớp Batch chưa chạy?)."
        except Exception as e:
            error = f"Lỗi khi đọc từ Cassandra: {e}"
            print(error, file=sys.stderr)

    return render_template('index.html', 
                           recs_list=recommendations, 
                           user_id_checked=user_id, 
                           lookup_error=error)

# === ROUTE 3: GỬI RATING (GHI VÀO KAFKA) ===
@app.route('/send_rating', methods=['POST'])
def handle_new_rating():
    # Tên input là 'user_id_rate', 'movie_id_rate', 'rating_rate'
    user_id = request.form.get('user_id_rate')
    movie_id = request.form.get('movie_id_rate')
    rating = request.form.get('rating_rate')
    
    try:
        send_new_rating(user_id, movie_id, rating)
        
        flash(f"Đã gửi rating (User={user_id}, Movie={movie_id}, Rating={rating}) vào Kafka! "
            f"Hãy 'Xem Gợi Ý' lại cho User {user_id} sau vài giây để thấy cập nhật.", 'success')
            
    except Exception as e:
        flash(f'Lỗi khi gửi: {e}', 'danger')

    return redirect(url_for('index')) # Quay lại trang chính

# === CHẠY APP ===
if __name__ == '__main__':
    if TMDB_API_KEY == "f725eef786cdf97fb091":
        print("="*50, file=sys.stderr)
        print("LỖI: Bạn chưa thay thế TMDB_API_KEY trong file app.py!", file=sys.stderr)
        print("="*50, file=sys.stderr)
    elif not MOVIE_MAP:
        print("="*50, file=sys.stderr)
        print("LỖI: Không tải được chi tiết phim. Kiểm tra đường dẫn file CSV.", file=sys.stderr)
        print(f"Đang tìm: {MOVIE_MAP_PATH} và {LINKS_MAP_PATH}", file=sys.stderr)
        print("Hãy chắc chắn bạn chạy app từ thư mục gốc (nơi có file docker-compose.yml)", file=sys.stderr)
        print("="*50, file=sys.stderr)
    else:
        # Chạy app trên cổng 5000, mở cho mọi IP
        app.run(host='0.0.0.0', port=5000, debug=True)