import json
import io

import pytest

from scripts import extract_breweries


class DummyResponse:
    def __init__(self, json_data, status=200):
        self._json = json_data
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")

    def json(self):
        return self._json


class DummyMinioClient:
    def __init__(self):
        self.objects = {}

    def put_object(self, bucket_name, object_name, data, length, content_type):
        self.objects[(bucket_name, object_name)] = data.read()


def test_fetch_all_breweries_paginates(monkeypatch):
    pages = [
        [{"id": 1}, {"id": 2}],
        [{"id": 3}],
        [],
    ]

    def fake_get(url, headers=None, params=None):
        page = params.get("page", 1) - 1
        return DummyResponse(pages[page])

    monkeypatch.setattr(extract_breweries.requests, "get", fake_get)

    result = extract_breweries.fetch_all_breweries()
    assert len(result) == 3
    assert result[0]["id"] == 1
    assert result[-1]["id"] == 3


def test_save_raw_data_to_s3_writes_json(monkeypatch):
    client = DummyMinioClient()
    data = [{"id": 1, "name": "Test Brewery", "brewery_type": "micro"}]

    extract_breweries.save_raw_data_to_s3(client, "bucket", "path/file.json", data)

    assert ("bucket", "path/file.json") in client.objects
    output = client.objects[("bucket", "path/file.json")].decode("utf-8")
    assert "\"id\": 1" in output
    assert "\"name\": \"Test Brewery\"" in output


def test_save_raw_data_to_s3_rejects_invalid_record(monkeypatch):
    client = DummyMinioClient()
    data = [{"id": 1, "name": "Test Brewery"}]  # missing brewery_type

    with pytest.raises(ValueError):
        extract_breweries.save_raw_data_to_s3(client, "bucket", "path/file.json", data)
