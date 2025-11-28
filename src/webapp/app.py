# src/webapp/app.py
import sys
import os
from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import requests


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


from utils.cassandra_connector import get_cassandra_session, read_recs, create_keyspace_and_table
create_keyspace_and_table()
from scripts.kafka_producer import send_new_rating

app = Flask(__name__)
app.secret_key = 'bigdata_secret_key'

TMDB_API_KEY = "f725eef7825ef832dc7836cdf97fb091"
TMDB_POSTER_BASE_URL = "https://image.tmdb.org/t/p/w200"
TMDB_POSTER_BASE_URL_W500 = "https://image.tmdb.org/t/p/w500"
POSTER_CACHE = {} 
MOVIE_MAP = {} 
MOVIE_DETAILS_CACHE = {}
def get_rich_movie_details(tmdb_id):
    if not tmdb_id or pd.isna(tmdb_id):
        return None

    if tmdb_id in MOVIE_DETAILS_CACHE:
        return MOVIE_DETAILS_CACHE[tmdb_id]
        
    try:
        api_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=vi-VN"
        response = requests.get(api_url, timeout=5)
        response.raise_for_status()
        
        data = response.json()
  
        details = {
            "overview": data.get('overview', 'Không có mô tả.'),
            "release_date": data.get('release_date', 'Không rõ'),
            "tagline": data.get('tagline', ''),
            "poster_url_large": f"{TMDB_POSTER_BASE_URL_W500}{data.get('poster_path')}" if data.get('poster_path') else None
        }
        
        MOVIE_DETAILS_CACHE[tmdb_id] = details
        return details
        
    except Exception as e:
        print(f"Lỗi khi gọi API TMDb chi tiết cho ID {tmdb_id}: {e}", file=sys.stderr)
        MOVIE_DETAILS_CACHE[tmdb_id] = None 
        return None
@app.route('/movie/<int:movie_id>')
def movie_details(movie_id):
    movie_info = None
    rich_details = None
    error = None
    
    user_id = request.args.get('user_id', '')

    try:
        movie_info = MOVIE_MAP.get(movie_id)
        
        if movie_info:
            tmdb_id = movie_info.get('tmdbId')
            rich_details = get_rich_movie_details(tmdb_id)

            movie_info['id'] = movie_id 
            movie_info.update(rich_details if rich_details else {})
            
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

print("Initializing Cassandra Session...")
get_cassandra_session() 

print("Loading movie details map...")
MOVIE_MAP_PATH = 'data/ml-32m/ml-32m/movies.csv'
LINKS_MAP_PATH = 'data/ml-32m/ml-32m/links.csv'
MOVIE_MAP = load_movie_details(MOVIE_MAP_PATH, LINKS_MAP_PATH)
print("Web App is ready.")

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/get_recs', methods=['POST'])
def get_recommendations():
    user_id = request.form.get('user_id_lookup')
    recommendations = []
    error = None
    
    if not user_id:
        error = "Vui lòng nhập User ID để tra cứu."
    else:
        try:
            movie_id_list = read_recs(user_id) 
            
            if movie_id_list:
                filtered_recs = []
                for movie_id in movie_id_list:
                    movie_details = MOVIE_MAP.get(int(movie_id))
                    if not movie_details:
                        continue
                    title = movie_details.get('title')
                    if not title or title.startswith('Không rõ tên'):
                        continue
                    tmdb_id = movie_details.get('tmdbId')
                    poster_url = get_poster_url(tmdb_id) or 'https://via.placeholder.com/200x300.png?text=No+Image'

                    filtered_recs.append({
                        "id": movie_id,
                        "title": title,
                        "poster_url": poster_url
                    })

                recommendations = filtered_recs[-10:]
            else:
                error = f"Không tìm thấy gợi ý cho User {user_id} (Lớp Batch chưa chạy?)."
        except Exception as e:
            error = f"Lỗi khi đọc từ Cassandra: {e}"
            print(error, file=sys.stderr)

    return render_template('index.html', 
                           recs_list=recommendations, 
                           user_id_checked=user_id, 
                           lookup_error=error)

@app.route('/send_rating', methods=['POST'])
def handle_new_rating():
    user_id = request.form.get('user_id_rate')
    movie_id = request.form.get('movie_id_rate')
    rating = request.form.get('rating_rate')
    
    try:
        send_new_rating(user_id, movie_id, rating)
        
        flash(f"Đã gửi rating (User={user_id}, Movie={movie_id}, Rating={rating}) vào Kafka! "
            f"Hãy 'Xem Gợi Ý' lại cho User {user_id} sau vài giây để thấy cập nhật.", 'success')
            
    except Exception as e:
        flash(f'Lỗi khi gửi: {e}', 'danger')

    return redirect(url_for('index')) 
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
        app.run(host='0.0.0.0', port=5000, debug=True)