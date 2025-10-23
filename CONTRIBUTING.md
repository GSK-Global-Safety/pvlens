# Contributing to PVLens

Thank you for your interest in contributing to PVLens! 
This project welcomes pull requests, bug reports, and documentation improvements.

---

## 1. Ways to Contribute
- **Report issues:** Use the GitHub *Issues* tab to report bugs or request enhancements.  
- **Improve documentation:** Typos, clarifications, and examples are always appreciated.  
- **Submit code changes:** Fork the repository and open a pull request (PR) to the `main` branch.

---

## 2. Development Setup

### Requirements
- Java 17+
- Apache Maven 3.8+
- Local UMLS installation (with MedDRA, SNOMED CT, RxNorm, ATC, etc.)
- Local SPL XML archive (downloaded from [DailyMed](https://dailymed.nlm.nih.gov))

### Build
mvn clean package -DskipTests

### RUN
java -jar target/pvlens-*.jar


## 3. Code Standards

Follow standard Java style and include Javadoc for public methods.

Keep methods cohesive; prefer readability over premature optimization.

All log output must use the central Logger class.

Do not include any proprietary data, credentials, or non-public UMLS resources in PRs.

Security fixes should never be discussed publicly until patched.

## 4. Testing

Add or update unit tests for all major changes.

Run mvn test before submitting your PR.

Ensure XML and SQL outputs are deterministic when possible.

## 5. Licensing and CLA

All contributions to PVLens are licensed under GPL v3.
By submitting a pull request, you confirm that your contribution is your own work and that you have the right to license it under GPL v3.

## 6. Review Process

Submit a pull request to the main branch.

A maintainer will review for coding standards, documentation, and testing.

Approved PRs are merged after all checks pass (CI build, OWASP scan, tests).

## 7. Code of Conduct

By participating, you agree to follow the PVLens Code of Conduct.
