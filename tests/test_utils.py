"""
Tests for utility functions
"""

import hashlib

import pytest

from src.utils import calculate_file_checksum, generate_month_range


class TestGenerateMonthRange:
    """Tests for generate_month_range function"""

    def test_single_month(self):
        """Test generating a single month"""
        result = generate_month_range("2024-01", "2024-01")
        assert result == [(2024, 1)]

    def test_multiple_months_same_year(self):
        """Test generating multiple months in same year"""
        result = generate_month_range("2024-01", "2024-03")
        assert result == [(2024, 1), (2024, 2), (2024, 3)]

    def test_months_across_years(self):
        """Test generating months across year boundary"""
        result = generate_month_range("2023-11", "2024-02")
        assert result == [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]

    def test_full_year(self):
        """Test generating all months in a year"""
        result = generate_month_range("2024-01", "2024-12")
        assert len(result) == 12
        assert result[0] == (2024, 1)
        assert result[-1] == (2024, 12)

    def test_reverse_dates_should_return_empty(self):
        """Test that reverse date order returns empty list"""
        result = generate_month_range("2024-03", "2024-01")
        assert result == []


class TestCalculateFileChecksum:
    """Tests for calculate_file_checksum function"""

    def test_checksum_empty_file(self, test_data_dir):
        """Test checksum of empty file"""
        empty_file = test_data_dir / "empty.txt"
        empty_file.write_text("")

        checksum = calculate_file_checksum(empty_file)

        # Empty file SHA256
        expected = hashlib.sha256(b"").hexdigest()
        assert checksum == expected

    def test_checksum_text_file(self, test_data_dir):
        """Test checksum of text file"""
        test_file = test_data_dir / "test.txt"
        test_content = "Hello, World!"
        test_file.write_text(test_content)

        checksum = calculate_file_checksum(test_file)

        expected = hashlib.sha256(test_content.encode()).hexdigest()
        assert checksum == expected

    def test_checksum_binary_file(self, test_data_dir):
        """Test checksum of binary file"""
        test_file = test_data_dir / "test.bin"
        test_content = b"\x00\x01\x02\x03\x04\x05"
        test_file.write_bytes(test_content)

        checksum = calculate_file_checksum(test_file)

        expected = hashlib.sha256(test_content).hexdigest()
        assert checksum == expected

    def test_checksum_consistency(self, test_data_dir):
        """Test that checksum is consistent for same file"""
        test_file = test_data_dir / "consistent.txt"
        test_file.write_text("Consistent content")

        checksum1 = calculate_file_checksum(test_file)
        checksum2 = calculate_file_checksum(test_file)

        assert checksum1 == checksum2

    def test_checksum_different_content(self, test_data_dir):
        """Test that different content produces different checksum"""
        file1 = test_data_dir / "file1.txt"
        file2 = test_data_dir / "file2.txt"

        file1.write_text("Content 1")
        file2.write_text("Content 2")

        checksum1 = calculate_file_checksum(file1)
        checksum2 = calculate_file_checksum(file2)

        assert checksum1 != checksum2

    def test_nonexistent_file_raises_error(self, test_data_dir):
        """Test that nonexistent file raises error"""
        nonexistent = test_data_dir / "does_not_exist.txt"

        with pytest.raises(FileNotFoundError):
            calculate_file_checksum(nonexistent)
