import heapq
from typing import Dict, Iterable, List, Sequence

import numpy as np


def solve_user_factor(
    item_vectors: Sequence[Sequence[float]],
    ratings: Sequence[float],
    reg: float = 0.1,
) -> List[float]:
    """
    Giải hệ least-squares để tìm user factor mới.
    item_vectors: danh sách vector của các movie mà user vừa rating (kích thước rank)
    ratings: danh sách rating tương ứng
    reg: hệ số regularization (giống regParam của ALS)
    """
    if not item_vectors:
        raise ValueError("Không có item vector để cập nhật user factor.")

    I = np.asarray(item_vectors, dtype=np.float64)
    r = np.asarray(ratings, dtype=np.float64).reshape(-1, 1)

    rank = I.shape[1]
    A = I.T @ I + reg * np.eye(rank)
    b = I.T @ r
    try:
        u = np.linalg.solve(A, b)
    except np.linalg.LinAlgError:
        # Nếu ma trận singular, dùng pseudo inverse
        u = np.linalg.pinv(A) @ b

    return u.flatten().tolist()


def recommend_from_user_factor(
    item_factors_map: Dict[int, Sequence[float]],
    user_vector: Sequence[float],
    exclude_movie_ids: Iterable[int],
    top_k: int = 10,
) -> List[int]:
    """
    Tính top-k movieId dựa trên user vector mới.
    Duyệt toàn bộ item_factors_map (số lượng movies không quá lớn),
    dùng heap nhỏ để giữ top-k điểm cao nhất.
    """
    if top_k <= 0:
        return []

    user_vec = np.asarray(user_vector, dtype=np.float64)
    exclude_set = set(int(mid) for mid in exclude_movie_ids)

    heap: List[tuple[float, int]] = []
    for movie_id, item_vec in item_factors_map.items():
        if movie_id in exclude_set:
            continue
        score = float(np.dot(item_vec, user_vec))
        if len(heap) < top_k:
            heapq.heappush(heap, (score, movie_id))
        else:
            heapq.heappushpop(heap, (score, movie_id))

    heap.sort(reverse=True)
    return [movie_id for score, movie_id in heap]

