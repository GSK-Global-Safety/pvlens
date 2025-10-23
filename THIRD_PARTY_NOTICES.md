# Third-Party Notices for PVLens

PVLens includes or depends on several open-source components and public-domain resources.
This document provides the required attributions and licensing statements.

---

## 1. FDA Structured Product Label (SPL) and SRLC Data
- **Source:** U.S. Food and Drug Administration / NIH DailyMed
- **License:** Public Domain (17 U.S.C. § 105)
- **Notes:** PVLens does not redistribute SPL or SRLC content. Users must download SPL archives directly from [https://dailymed.nlm.nih.gov](https://dailymed.nlm.nih.gov).
  Manufacturer trademarks and phone numbers may appear within extracted text.

---

## 2. UMLS, MedDRA, SNOMED CT, RxNorm, ATC, MTHSPL
- **Source:** U.S. National Library of Medicine (UMLS Metathesaurus)
- **License:** Restricted – requires a valid UMLS license from NLM.
- **Notes:** These vocabularies are *not included* with PVLens.
  Users are responsible for compliance with all NLM and MedDRA licensing terms.

---

## 3. Stopword List (LHNCBC / fda-ars)
- **Component:** `MetaMapFiles/DATA/StopList`
- **Source:** [https://github.com/LHNCBC/fda-ars](https://github.com/LHNCBC/fda-ars)
- **License:** BSD-style with U.S. Government usage rights 
- **Required Notice:** 
  “Portions of this work are derived from materials provided courtesy of the U.S. National Library of Medicine.” 
- **PVLens modifications:** Additional stop words © 2025 GlaxoSmithKline, released under GPL v3.

---

## 4. Apache OpenNLP Models
- **Files:** `en-sent.bin`, `en-token.bin` 
- **Source:** [https://opennlp.apache.org](https://opennlp.apache.org) 
- **License:** Apache License 2.0 
- **Copyright:** © The Apache Software Foundation

---

## 5. Java Libraries (via Maven)

| Library | License | URL |
|----------|----------|-----|
| Apache Commons Lang / CSV / IO | Apache 2.0 | https://commons.apache.org |
| Jsoup | MIT | https://jsoup.org |
| MySQL Connector/J | GPL v2 + FOSS Exception | https://dev.mysql.com |
| H2 Database | EPL 1.0 / MPL 1.1 | https://h2database.com |
| Lombok | MIT | https://projectlombok.org |
| JAXB (javax.xml.bind) | Dual CDDL 1.1 / GPL v2 + Classpath Exception | https://javaee.github.io/jaxb-v2/ |
| Apache POI | Apache 2.0 | https://poi.apache.org |
| JUnit 5 | EPL 2.0 | https://junit.org |
| Mockito | MIT | https://site.mockito.org |
| OWASP Dependency-Check | Apache 2.0 | https://owasp.org |
| CycloneDX Maven Plugin | Apache 2.0 | https://cyclonedx.org |

---

## 6. Public-Domain / Government Works
Some content processed by PVLens (e.g., SPL XML and SRLC HTML) originates from U.S. Government works and is in the public domain.

---

## 7. Excluded / Non-Redistributable Materials
| Dataset / Vocabulary | License | Included? | Notes |
|----------------------|----------|-----------|-------|
| UMLS / MedDRA / SNOMED CT / RxNorm / ATC | Proprietary / Restricted | ❌ Excluded | Must be licensed separately |
| Manufacturer package inserts (PDFs, HTML) | Various | ❌ Excluded | Processed locally by users only |

---

## 8. Attribution Summary
PVLens incorporates components under GPL v3, Apache 2.0, MIT, BSD-style, EPL/MPL, and public-domain terms. 
All third-party license texts are provided in the `licenses/` directory or may be obtained from their respective upstream sources.

---

© 2025 GlaxoSmithKline plc and contributors. 
PVLens is distributed under the GNU General Public License v3 (GPL v3).  
