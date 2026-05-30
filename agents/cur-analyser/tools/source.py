from abc import ABC, abstractmethod


class CurSource(ABC):
    @abstractmethod
    async def load_csv(self) -> str:
        """Return the CUR CSV as a raw text string."""
        ...


class FileSource(CurSource):
    """CUR data provided as a raw CSV string (uploaded file stored in DB as TEXT)."""

    def __init__(self, raw_csv: str) -> None:
        self._raw_csv = raw_csv

    async def load_csv(self) -> str:
        return self._raw_csv


class S3Source(CurSource):
    """Phase 2 stub — direct S3 CUR manifest connectivity."""

    def __init__(self, bucket: str, prefix: str, aws_region: str = "us-east-1") -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._aws_region = aws_region

    async def load_csv(self) -> str:
        raise NotImplementedError("S3 direct connectivity is not yet implemented (Phase 2).")


class SyntheticSource(CurSource):
    """Generates realistic synthetic AWS CUR data for testing and demos."""

    async def load_csv(self) -> str:
        # generate_sample_csv is defined in tools/sample_generator.py
        from tools.sample_generator import generate_sample_csv
        return await generate_sample_csv()
