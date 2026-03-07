"""Tests for FocusStore thread-safety and atomic write guarantees."""

from __future__ import annotations

import concurrent.futures
import json
import threading
from pathlib import Path

import pytest

from src.raven.focus import FocusDocument, FocusStore


@pytest.fixture
def store(tmp_path):
    """Create a FocusStore backed by a temp directory."""
    return FocusStore(base_dir=tmp_path)


@pytest.fixture
def sample_doc():
    return FocusDocument(
        id="test-001",
        name="Revenue Focus",
        description="Revenue-related tables",
        tables=["gold.finance.orders", "gold.finance.revenue"],
        business_rules=[{"rule": "Revenue = SUM(amount)"}],
    )


# ── Basic CRUD ─────────────────────────────────────────────────


class TestFocusStoreCRUD:
    def test_create_and_get(self, store, sample_doc):
        store.create_document(sample_doc)
        retrieved = store.get_document("test-001")
        assert retrieved is not None
        assert retrieved.name == "Revenue Focus"
        assert len(retrieved.tables) == 2

    def test_list_documents(self, store, sample_doc):
        store.create_document(sample_doc)
        docs = store.list_documents()
        assert len(docs) == 1
        assert docs[0]["name"] == "Revenue Focus"

    def test_update_document(self, store, sample_doc):
        store.create_document(sample_doc)
        updated = store.update_document("test-001", {"name": "Updated Focus"})
        assert updated is not None
        assert updated.name == "Updated Focus"
        # Verify persistence
        re_read = store.get_document("test-001")
        assert re_read.name == "Updated Focus"

    def test_delete_document(self, store, sample_doc):
        store.create_document(sample_doc)
        assert store.delete_document("test-001") is True
        assert store.get_document("test-001") is None

    def test_get_nonexistent(self, store):
        assert store.get_document("nonexistent") is None

    def test_delete_nonexistent(self, store):
        assert store.delete_document("nonexistent") is False

    def test_update_nonexistent(self, store):
        assert store.update_document("nonexistent", {"name": "x"}) is None


# ── Atomic write guarantees ────────────────────────────────────


class TestAtomicWrite:
    def test_file_exists_after_create(self, store, sample_doc, tmp_path):
        store.create_document(sample_doc)
        file_path = tmp_path / "test-001.json"
        assert file_path.exists()
        data = json.loads(file_path.read_text())
        assert data["name"] == "Revenue Focus"

    def test_no_temp_files_left(self, store, sample_doc, tmp_path):
        """Atomic write should not leave .tmp files behind."""
        store.create_document(sample_doc)
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []


# ── Thread-safety tests ───────────────────────────────────────


class TestThreadSafety:
    def test_concurrent_creates(self, store):
        """Multiple threads creating different documents should not corrupt data."""
        def create_doc(i: int):
            doc = FocusDocument(
                id=f"thread-{i:03d}",
                name=f"Doc {i}",
                tables=[f"table_{i}"],
            )
            store.create_document(doc)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(create_doc, i) for i in range(20)]
            concurrent.futures.wait(futures)
            # Check for exceptions
            for f in futures:
                f.result()

        docs = store.list_documents()
        assert len(docs) == 20

    def test_concurrent_updates(self, store, sample_doc):
        """Multiple threads updating the same document should not lose data."""
        store.create_document(sample_doc)

        def update_doc(i: int):
            store.update_document("test-001", {"description": f"Updated by thread {i}"})

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(update_doc, i) for i in range(20)]
            concurrent.futures.wait(futures)
            for f in futures:
                f.result()

        doc = store.get_document("test-001")
        assert doc is not None
        # Description should be from one of the threads (deterministic, last-writer-wins)
        assert doc.description.startswith("Updated by thread")

    def test_concurrent_read_write(self, store, sample_doc):
        """Concurrent reads during writes should not give corrupt data."""
        store.create_document(sample_doc)
        errors = []

        def writer():
            for i in range(10):
                store.update_document("test-001", {"description": f"Write {i}"})

        def reader():
            for _ in range(10):
                doc = store.get_document("test-001")
                if doc is None:
                    errors.append("Got None during concurrent read")
                elif not isinstance(doc.name, str):
                    errors.append(f"Corrupt name: {doc.name}")

        threads = [threading.Thread(target=writer) for _ in range(3)]
        threads += [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Concurrent read/write errors: {errors}"


# ── Suggestion thread-safety ──────────────────────────────────


class TestSuggestionThreadSafety:
    def test_concurrent_add_suggestions(self, store, sample_doc):
        store.create_document(sample_doc)

        def add_suggestion(i: int):
            store.add_suggestion(
                document_id="test-001",
                suggestion_type="add_table",
                suggestion_data={"table": f"table_{i}"},
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(add_suggestion, i) for i in range(15)]
            concurrent.futures.wait(futures)
            for f in futures:
                f.result()

        suggestions = store.list_suggestions(document_id="test-001")
        assert len(suggestions) == 15

    def test_review_suggestion(self, store, sample_doc):
        store.create_document(sample_doc)
        entry = store.add_suggestion(
            document_id="test-001",
            suggestion_type="add_table",
            suggestion_data={"table": "gold.new_table"},
        )
        result = store.review_suggestion(entry["id"], "accepted")
        assert result is not None
        assert result["status"] == "accepted"

        # Verify the table was added to the document
        doc = store.get_document("test-001")
        assert "gold.new_table" in doc.tables
