# PVLens Security Policy

## Overview
PVLens is an offline, file-centric batch framework designed to process publicly available FDA SPL XML data.  
It does not transmit, store, or process personally identifiable information (PII) or protected health information (PHI).

---

## Supported Versions
Security updates apply to the latest stable branch (main) and tagged releases published on the official repository.

| Version | Supported |
|----------|------------|
| 1.x (current) | ✅ |
| pre-1.0 beta | ❌ |

---

## Reporting a Vulnerability
If you believe you have found a security issue in PVLens:

1. **Do not** open a public GitHub issue.  
2. Email the maintainers at **security@pvlens.org** (or through your GSK open-source security contact).  
3. Provide a concise description of the issue, including:
   - Affected version or commit hash  
   - Steps to reproduce  
   - Expected and actual behavior  
   - Suggested fix or mitigation, if known

We will acknowledge receipt within **5 business days** and coordinate a responsible disclosure timeline.

---

## Secure Development Practices
PVLens incorporates multiple hardening measures:

| Area | Current Status |
|------|----------------|
| **XML Parsing** | XXE and DTD disabled; external entities and XInclude blocked |
| **Database Connectivity** | TLS enforced (`sslMode=VERIFY_IDENTITY`); no `useSSL=false` allowed |
| **Secrets Management** | No embedded credentials; environment variable overrides supported |
| **Logging** | Centralized logger; UUIDs and paths redacted in normal mode |
| **SQL Generation** | Offline SQL artifacts only; runtime DB writes use prepared statements |
| **Dependency Scanning** | OWASP Dependency-Check 12.1.6 + CycloneDX SBOM integrated |
| **Model Integrity** | SHA-256 checksums published for NLP model binaries |
| **Input Validation** | Canonical path enforcement and size limits on XML/CSV ingestion |

---

## Threat Model Summary
PVLens operates in a trusted local environment. Primary risks involve:
- **XML parser attacks** (XXE, entity expansion) → mitigated by secure factory settings  
- **Path traversal** during file discovery → mitigated by canonical-path checks  
- **SQL or code injection** → mitigated by prepared statements and static artifacts  
- **Dependency vulnerabilities** → mitigated by regular OWASP scans  
- **Information disclosure** via logs → mitigated by redaction and verbosity controls  

---

## Security Contacts
- **Primary Maintainer:** PVLens Team (GSK Data Science / Pharmacovigilance)  
- **Email:** security@pvlens.org  
- **Advisory Policy:** Coordinated vulnerability disclosure (CVD) following ISO/IEC 29147.

---

© 2025 GlaxoSmithKline plc and contributors.  
PVLens is provided “as is,” without warranty or guarantee of any kind.
