from typing import List
from sentence_transformers import SentenceTransformer

_model_cache = {}

def embed_texts(texts: List[str], model_name: str, normalize: bool = True) -> List[List[float]]:
    if model_name not in _model_cache:
        _model_cache[model_name] = SentenceTransformer(model_name)
    model = _model_cache[model_name]

    vecs = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=False,
        normalize_embeddings=normalize,
    )
    # Convert to python lists
    return [v.tolist() for v in vecs]
