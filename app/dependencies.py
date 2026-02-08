from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.pipeline.ask import QuestionPipeline
    from app.pipeline.ingest.pdf.services.topic_classifier import PDFTopicClassifier
    from app.pipeline.upload.service import UploadService


def build_topic_classifier() -> "PDFTopicClassifier":
    from app.pipeline.ingest.pdf.services.topic_classifier import PDFTopicClassifier

    return PDFTopicClassifier()


def build_question_pipeline(*, db_path: Path) -> "QuestionPipeline":
    from app.pipeline.ask import QuestionPipeline

    return QuestionPipeline(db_path=db_path)


def build_upload_service(
    *,
    db_path: Path,
    upload_pdf_dir: Path = Path("data/uploads/pdf"),
) -> "UploadService":
    from app.pipeline.upload.service import UploadService

    return UploadService(
        db_path=db_path,
        upload_pdf_dir=upload_pdf_dir,
        topic_classifier=build_topic_classifier(),
    )
