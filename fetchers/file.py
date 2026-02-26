"""File fetcher for JSON, XML and other structured file formats."""

import json
import time
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

import aiofiles

from fetchers.base import BaseFetcher, FetchResult
from security import validate_file_path


class FileFetcher(BaseFetcher):
    """Fetcher for structured file sources (JSON, XML, etc.)."""

    async def fetch(self, session) -> FetchResult:
        """Fetch data from local file."""
        if not self.config.path:
            return FetchResult(
                source_name=self.config.name,
                error="File source requires path"
            )

        try:
            file_path = self.config.path
            
            # Validate file path (detailed runtime validation with allowed dirs 
            # will be done by pipeline)
            validate_file_path(file_path)
            
            start_time = time.time()
            
            path_obj = Path(file_path)
            
            if not path_obj.exists():
                return FetchResult(
                    source_name=self.config.name,
                    error=f"File not found: {file_path}"
                )
            
            if not path_obj.is_file():
                return FetchResult(
                    source_name=self.config.name,
                    error=f"Path is not a file: {file_path}"
                )
            
            # Check file size to prevent memory issues
            file_size = path_obj.stat().st_size
            if file_size > 50 * 1024 * 1024:  # 50MB limit
                return FetchResult(
                    source_name=self.config.name,
                    error=f"File too large: {file_size} bytes (max 50MB)"
                )
            
            # Read file content
            async with aiofiles.open(path_obj, mode='r', encoding='utf-8') as f:
                content = await f.read()
            
            fetch_time = (time.time() - start_time) * 1000
            
            # Parse file based on extension
            records = await self._parse_file_content(content, path_obj.suffix.lower())
            
            # Apply incremental filtering
            if self.config.incremental:
                records = self._filter_incremental_records(records)
            
            # Apply field mapping
            normalized_records = self._normalize_records(records)
            
            return FetchResult(
                source_name=self.config.name,
                records=normalized_records,
                fetch_time_ms=fetch_time
            )
            
        except Exception as e:
            return FetchResult(
                source_name=self.config.name,
                error=f"File fetch error: {str(e)}"
            )

    async def _parse_file_content(self, content: str, file_extension: str) -> list[dict[str, Any]]:
        """Parse file content based on file type."""
        if file_extension in ('.json', '.jsonl'):
            return await self._parse_json(content, file_extension)
        elif file_extension in ('.xml',):
            return await self._parse_xml(content)
        else:
            # Try JSON first as fallback
            try:
                return await self._parse_json(content, '.json')
            except:
                # If JSON fails, treat as text
                return [{"content": content, "type": "text"}]

    async def _parse_json(self, content: str, extension: str) -> list[dict[str, Any]]:
        """Parse JSON or JSONL content."""
        if extension == '.jsonl':
            # JSON Lines format - each line is a separate JSON object
            records = []
            for line_num, line in enumerate(content.strip().split('\n'), 1):
                if line.strip():
                    try:
                        record = json.loads(line.strip())
                        if isinstance(record, dict):
                            records.append(record)
                        elif isinstance(record, list):
                            records.extend([r for r in record if isinstance(r, dict)])
                    except json.JSONDecodeError as e:
                        # Skip invalid lines but continue processing
                        continue
                        
                # Limit to prevent memory issues
                if len(records) >= 10000:
                    break
            
            return records
        
        else:
            # Regular JSON
            try:
                data = json.loads(content)
                return self._extract_records_from_json(data)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON: {str(e)}")

    async def _parse_xml(self, content: str) -> list[dict[str, Any]]:
        """Parse XML content into records."""
        try:
            root = ET.fromstring(content)
            return self._xml_to_records(root)
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML: {str(e)}")

    def _extract_records_from_json(self, data: Any) -> list[dict[str, Any]]:
        """Extract records from parsed JSON data."""
        if isinstance(data, list):
            # Array of records
            return [record for record in data if isinstance(record, dict)]
        
        elif isinstance(data, dict):
            # Look for common array field names
            for key in ['data', 'items', 'records', 'results']:
                if key in data and isinstance(data[key], list):
                    return [record for record in data[key] if isinstance(record, dict)]
            
            # Single record
            return [data]
        
        else:
            # Primitive value, wrap it
            return [{"value": data}]

    def _xml_to_records(self, element: ET.Element) -> list[dict[str, Any]]:
        """Convert XML element tree to list of records."""
        records = []
        
        # If root has multiple children of the same tag, treat each as a record
        child_tags = {}
        for child in element:
            tag = child.tag
            child_tags[tag] = child_tags.get(tag, 0) + 1
        
        # Find most common child tag (likely the record container)
        if child_tags:
            most_common_tag = max(child_tags.items(), key=lambda x: x[1])[0]
            
            if child_tags[most_common_tag] > 1:
                # Multiple children with same tag - treat each as a record
                for child in element:
                    if child.tag == most_common_tag:
                        record = self._xml_element_to_dict(child)
                        records.append(record)
            else:
                # Single instances - convert whole tree
                record = self._xml_element_to_dict(element)
                records.append(record)
        else:
            # No children - leaf element
            record = self._xml_element_to_dict(element)
            records.append(record)
        
        return records

    def _xml_element_to_dict(self, element: ET.Element) -> dict[str, Any]:
        """Convert XML element to dictionary."""
        result = {}
        
        # Add attributes
        if element.attrib:
            for key, value in element.attrib.items():
                result[f"@{key}"] = value
        
        # Add text content if present
        if element.text and element.text.strip():
            result["text"] = element.text.strip()
        
        # Add children
        child_dict = {}
        for child in element:
            child_data = self._xml_element_to_dict(child)
            
            if child.tag in child_dict:
                # Multiple children with same tag - make it a list
                if not isinstance(child_dict[child.tag], list):
                    child_dict[child.tag] = [child_dict[child.tag]]
                child_dict[child.tag].append(child_data)
            else:
                child_dict[child.tag] = child_data
        
        result.update(child_dict)
        
        # If result only has text, return just the text value
        if len(result) == 1 and "text" in result:
            return result["text"]
        
        return result

    def _filter_incremental_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter records for incremental updates."""
        last_value = self.get_incremental_filter()
        if last_value is None:
            return records
        
        field = self.config.incremental_field
        filtered = []
        
        for record in records:
            if field not in record:
                continue
            
            record_value = record[field]
            
            # Compare based on value type
            try:
                if isinstance(last_value, (int, float)) and isinstance(record_value, (int, float)):
                    if record_value > last_value:
                        filtered.append(record)
                elif isinstance(last_value, str) and isinstance(record_value, str):
                    if record_value > last_value:
                        filtered.append(record)
                else:
                    # Type mismatch, include record to be safe
                    filtered.append(record)
            except (TypeError, ValueError):
                # Comparison failed, include record
                filtered.append(record)
        
        return filtered

    def supports_incremental(self) -> bool:
        """Check if this fetcher supports incremental updates."""
        return True


# Export the fetcher
__all__ = ['FileFetcher']