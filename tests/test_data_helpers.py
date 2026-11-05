import hashlib
import tempfile
import unittest
from pathlib import Path

from src.download_data import file_sha256, format_bytes
from src.schema_inspector import collect_paths, json_type


class DownloadHelpersTest(unittest.TestCase):
    def test_file_sha256(self) -> None:
        content = b"acousticbrainz"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.bin"
            path.write_bytes(content)

            self.assertEqual(file_sha256(path), hashlib.sha256(content).hexdigest())

    def test_format_bytes(self) -> None:
        self.assertEqual(format_bytes(512), "512.0 B")
        self.assertEqual(format_bytes(2048), "2.0 KiB")


class SchemaInspectorTest(unittest.TestCase):
    def test_json_type_distinguishes_boolean_from_number(self) -> None:
        self.assertEqual(json_type(True), "boolean")
        self.assertEqual(json_type(1), "number")

    def test_collect_paths_merges_array_item_types(self) -> None:
        paths = collect_paths({"items": [{"value": 1}, {"value": "one"}]})

        self.assertEqual(paths["items"], {"array"})
        self.assertEqual(paths["items[]"], {"object"})
        self.assertEqual(paths["items[].value"], {"number", "string"})


if __name__ == "__main__":
    unittest.main()
