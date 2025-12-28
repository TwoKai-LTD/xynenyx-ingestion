"""Metadata extraction utilities (reused from RAG service)."""
import re
from typing import Dict, Any, List
from datetime import datetime
import dateparser


def normalize_name(name: str) -> str:
    """Normalize a name for matching (lowercase, remove special chars)."""
    return re.sub(r'[^a-zA-Z0-9\s]', '', name.lower().strip())


class MetadataExtractor:
    """Extract structured metadata from startup/VC content."""

    def __init__(self):
        """Initialize metadata extractor."""
        self.funding_patterns = [
            r"\$(\d+(?:\.\d+)?)\s*(?:million|M|billion|B|k|K)",
            r"€(\d+(?:\.\d+)?)\s*(?:million|M|billion|B|k|K)",
            r"£(\d+(?:\.\d+)?)\s*(?:million|M|billion|B|k|K)",
            r"raised\s+\$(\d+(?:\.\d+)?)\s*(?:million|M|billion|B|k|K)?",
            r"funding\s+of\s+\$(\d+(?:\.\d+)?)\s*(?:million|M|billion|B|k|K)?",
            r"secured\s+\$(\d+(?:\.\d+)?)\s*(?:million|M|billion|B|k|K)?",
            r"closed\s+(?:a\s+)?\$(\d+(?:\.\d+)?)\s*(?:million|M|billion|B|k|K)?",
        ]

        self.round_patterns = [
            r"(?:Seed|seed)\s+round",
            r"Series\s+([A-Z])\s+round",
            r"Series\s+([A-Z])\s+funding",
            r"([A-Z])\s+round",
        ]

        self.investor_patterns = [
            r"led\s+by\s+([A-Z][a-zA-Z\s&,]+)",
            r"investors\s+include\s+([A-Z][a-zA-Z\s&,]+)",
            r"backed\s+by\s+([A-Z][a-zA-Z\s&,]+)",
            r"invested\s+by\s+([A-Z][a-zA-Z\s&,]+)",
        ]

        self.date_patterns = [
            r"\d{4}-\d{2}-\d{2}",
            r"[A-Z][a-z]+\s+\d{1,2},\s+\d{4}",
            r"\d{1,2}\s+[A-Z][a-z]+\s+\d{4}",
        ]

    def extract(self, content: str, article_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured metadata from content."""
        companies = self._extract_companies(content)
        funding_amounts = self._extract_funding_amounts(content)
        dates = self._extract_dates(content)
        investors = self._extract_investors(content)
        sectors = self._extract_sectors(content)

        metadata = {
            "companies": companies,
            "funding_amounts": funding_amounts,
            "dates": dates,
            "investors": investors,
            "sectors": sectors,
        }

        metadata.update(article_metadata)
        return metadata

    def _extract_companies(self, content: str) -> List[str]:
        """Extract company names."""
        companies = set()

        # Pattern 1: Company name before action verbs (more specific)
        pattern1 = r"\b([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3})\s+(?:announced|raised|launched|secured|closed|revealed|said|reported)"
        matches1 = re.findall(pattern1, content)
        companies.update(matches1)

        # Pattern 2: Company/startup/firm followed by name
        pattern2 = r"(?:company|startup|firm|business|enterprise)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,2})"
        matches2 = re.findall(pattern2, content, re.IGNORECASE)
        companies.update(matches2)

        # Pattern 3: Company suffixes (Inc, Corp, Labs, etc.)
        pattern3 = r"\b([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,2}(?:Corp|Labs|Tech|AI|Systems|Solutions|Inc|LLC|LLP|Ltd|Limited))\b"
        matches3 = re.findall(pattern3, content)
        companies.update(matches3)

        # Pattern 4: "X, a Y company" or "X, which..."
        pattern4 = r"\b([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,2}),\s+(?:a|an|the)\s+"
        matches4 = re.findall(pattern4, content)
        companies.update(matches4)

        # Extended false positives
        false_positives = {
            "The", "This", "That", "These", "Those", "Today", "Yesterday",
            "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
            "January", "February", "March", "April", "May", "June", "July", 
            "August", "September", "October", "November", "December",
            "New York", "San Francisco", "Los Angeles", "United States",
            "Funding Rounds", "Funding Round", "Startup Funding", "Venture Capital",
            "Series A", "Series B", "Series C", "Seed Round",
        }
        
        # Filter out false positives and validate
        filtered = set()
        for c in companies:
            # Must be at least 3 characters
            if len(c) < 3:
                continue
            # Must not be in false positives
            if c in false_positives:
                continue
            # Must not be a common word (check if all words are capitalized)
            words = c.split()
            if len(words) > 1:
                # Multi-word names should have at least one word > 3 chars
                if all(len(w) <= 3 for w in words):
                    continue
            # Must not start with common articles/prepositions
            if c.split()[0].lower() in ["the", "a", "an", "in", "on", "at", "for", "with", "to", "that", "this", "these", "those"]:
                continue
            # Must not be a date pattern
            if re.match(r"^\d{1,2}\s+[A-Z]", c):
                continue
            # Filter out common verb phrases (likely false positives)
            verb_phrases = ["to fix", "to become", "to try", "to use", "to make", "to build", "to pursue", "to produce", "to search", "to lose", "to diversify"]
            if any(c.lower().startswith(phrase) for phrase in verb_phrases):
                continue
            # Filter out phrases starting with common words
            if c.split()[0].lower() in ["skipped", "with", "should", "continues", "said", "will", "is", "has", "had", "was", "were", "can", "could", "would"]:
                continue
            # Filter out very long phrases (4+ words are usually false positives)
            if len(c.split()) > 3:
                continue
            filtered.add(c)

        return list(filtered)[:15]

    def _extract_funding_amounts(self, content: str) -> List[Dict[str, Any]]:
        """Extract funding amounts with round information."""
        amounts = []
        seen = set()

        for pattern in self.funding_patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                value = float(match.group(1))
                unit_text = match.group(0).lower()

                if "billion" in unit_text or "B" in unit_text:
                    value = value * 1000
                elif "k" in unit_text or "K" in unit_text:
                    value = value / 1000

                currency = "USD"
                if "€" in match.group(0):
                    currency = "EUR"
                elif "£" in match.group(0):
                    currency = "GBP"

                round_info = self._extract_round_nearby(content, match.start(), match.end())

                amount_key = (value, currency, round_info)
                if amount_key not in seen:
                    seen.add(amount_key)
                    amounts.append({
                        "amount_millions": value,
                        "currency": currency,
                        "round": round_info,
                        "position": match.start(),  # Store position for proximity matching
                    })

        return amounts[:5]

    def _extract_round_nearby(self, content: str, start: int, end: int) -> str | None:
        """Extract funding round information near a funding amount."""
        window_start = max(0, start - 50)
        window_end = min(len(content), end + 50)
        window = content[window_start:window_end]

        for pattern in self.round_patterns:
            match = re.search(pattern, window, re.IGNORECASE)
            if match:
                if match.group(0).startswith("Series"):
                    return f"Series {match.group(1)}"
                elif "Seed" in match.group(0):
                    return "Seed"
        return None

    def _extract_dates(self, content: str) -> List[str]:
        """Extract dates from content."""
        dates = []
        seen = set()

        for pattern in self.date_patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                parsed = dateparser.parse(match)
                if parsed:
                    iso_date = parsed.isoformat()
                    if iso_date not in seen:
                        seen.add(iso_date)
                        dates.append(iso_date)

        return dates[:10]

    def _extract_investors(self, content: str) -> List[Dict[str, Any]]:
        """Extract investor names with role identification."""
        investors = []
        seen = set()

        for pattern in self.investor_patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                investor_text = match.group(1).strip()
                investor_list = re.split(r"[,\s]+and\s+|\s*,\s*", investor_text)

                is_lead = "led by" in match.group(0).lower()

                for inv in investor_list:
                    inv_clean = inv.strip()
                    if inv_clean and inv_clean not in seen:
                        seen.add(inv_clean)
                        investors.append({
                            "name": inv_clean,
                            "role": "lead" if is_lead and len(investors) == 0 else "participant",
                        })

        return investors[:20]

    def _extract_sectors(self, content: str) -> List[Dict[str, Any]]:
        """Extract sectors/industries."""
        sectors = [
            "AI", "Machine Learning", "FinTech", "HealthTech", "SaaS",
            "E-commerce", "Cybersecurity", "EdTech", "Climate Tech", "Biotech",
            "Enterprise Software", "Consumer", "Blockchain", "Web3", "Gaming",
            "Media", "Transportation", "Real Estate", "Food & Beverage", "Fashion",
        ]

        found_sectors = []
        content_lower = content.lower()

        for sector in sectors:
            sector_lower = sector.lower()
            count = content_lower.count(sector_lower)
            if count > 0:
                confidence = min(1.0, 0.5 + (count * 0.1))
                found_sectors.append({
                    "sector": sector,
                    "confidence": confidence,
                })

        found_sectors.sort(key=lambda x: x["confidence"], reverse=True)
        return found_sectors[:10]

