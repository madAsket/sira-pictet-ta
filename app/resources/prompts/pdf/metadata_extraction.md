# PDF Metadata Extraction Prompt

You are extracting bibliographic metadata from a PDF report.

Return STRICT JSON only, with this exact schema:
{
  "title": string | null,
  "publisher": string | null,
  "year": integer | null,
  "confidence": number,
  "evidence": {
    "title_line": string | null,
    "publisher_line": string | null,
    "year_line": string | null
  }
}

Rules:
- Do not invent facts. If missing, return null.
- Use file name only as a hint, not as source of truth.
- Year must be explicit in text.
- Prefer years near "Published", "Report", or "©".
- If only weak year evidence exists, set lower confidence.

File name hint: {{file_name}}

Raw text from first pages:
{{preview_text}}
