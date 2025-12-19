"""Context objects used by the data connector."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .utils import slugify


@dataclass(slots=True)
class DataSetContext:
    """Container describing a row extracted from the data map spreadsheet."""

    tech_lead: str
    domain_type: str
    domain_name: str
    physical_name: str
    options: Mapping[str, Any]

    def slug(self) -> str:
        """Return a filesystem-safe slug that represents the dataset."""

        candidate = (
            self.options.get("output_name") or self.physical_name or self.domain_name
        )
        return slugify(str(candidate))
