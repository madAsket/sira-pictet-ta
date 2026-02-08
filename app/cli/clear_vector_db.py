from __future__ import annotations

import argparse
import logging

from qdrant_client import QdrantClient, models

from app.core.settings import get_settings

LOGGER = logging.getLogger("clear_vector_db")


def clear_vector_collection(
    qdrant_url: str,
    collection_name: str,
    recreate: bool,
    vector_size: int,
) -> None:
    client = QdrantClient(url=qdrant_url)
    if client.collection_exists(collection_name):
        client.delete_collection(collection_name)
        LOGGER.info("Deleted collection: %s", collection_name)
    else:
        LOGGER.info("Collection does not exist: %s", collection_name)

    if recreate:
        client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=vector_size,
                distance=models.Distance.COSINE,
            ),
        )
        LOGGER.info(
            "Recreated collection: %s (vector_size=%s)",
            collection_name,
            vector_size,
        )


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Delete Qdrant collection used by PDF RAG.")
    parser.add_argument(
        "--qdrant-url",
        type=str,
        default=settings.qdrant_url,
        help="Qdrant URL.",
    )
    parser.add_argument(
        "--collection",
        type=str,
        default=settings.qdrant_collection,
        help="Qdrant collection name.",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Recreate collection after deletion.",
    )
    parser.add_argument(
        "--vector-size",
        type=int,
        default=3072,
        help="Vector size for --recreate (default for text-embedding-3-large).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    clear_vector_collection(
        qdrant_url=args.qdrant_url,
        collection_name=args.collection,
        recreate=args.recreate,
        vector_size=args.vector_size,
    )

if __name__ == "__main__":
    main()
