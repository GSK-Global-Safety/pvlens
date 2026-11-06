# PVLens API

A read-only REST API over a PVLens database. It serves the MySQL database that
`pvlens-db` builds, so other tools can query labeled adverse events,
indications, label text and safety label changes without writing SQL.

The API never runs the pipeline and never needs UMLS or SPL files. It only
needs SELECT access to a loaded `pvlens` database.

## Licensing notice

A PVLens database contains MedDRA terms and UMLS-derived vocabularies
(RxNorm, SNOMED CT, ATC). Serving them to people who are not covered by your
MedDRA subscription or UMLS license may count as redistribution. For that
reason the API requires an API key, and refuses to start without one unless
you turn on anonymous mode explicitly for local development.

## Try it without a database

You only need Java 17 or newer (check with `java -version`). Maven is not
required: the `mvnw` / `mvnw.cmd` wrapper downloads it on first use.

```
git clone -b feature/rest-api https://github.com/chalamalasetti/pvlens
cd pvlens/pvlens-api
mvnw.cmd test               (Windows)      ./mvnw test               (macOS/Linux)
mvnw.cmd spring-boot:test-run              ./mvnw spring-boot:test-run
```

`test` runs the automated tests. `spring-boot:test-run` starts the API on the
small in-memory sample database used by the tests. Then open
http://localhost:8080/swagger-ui.html, click **Authorize**, enter `test-key-1`,
and use "Try it out" on any endpoint. Or:

```
curl -H "X-API-Key: test-key-1" "http://localhost:8080/v1/products?name=avodart"
```

The sample data is made up and tiny; it is only for trying the endpoints.

## Requirements

- Java 17+
- Maven 3.8+, or the included `mvnw` wrapper
- A loaded PVLens MySQL database (see the main README, section 6)

## Setup

1. Add the API indexes and a read-only user (once per database build):

   ```
   mysql -u root -p pvlens < src/main/resources/sql/api_indexes.sql
   mysql -u root -p -e "CREATE USER 'pvlens_ro'@'%' IDENTIFIED BY '<password>';
                        GRANT SELECT ON pvlens.* TO 'pvlens_ro'@'%';"
   ```

2. Build and run:

   ```
   mvn clean package
   PVLENS_DB_URL=jdbc:mysql://localhost:3306/pvlens \
   PVLENS_DB_USER=pvlens_ro \
   PVLENS_DB_PASSWORD=<password> \
   PVLENS_API_KEYS=<key1>,<key2> \
   java -jar target/pvlens-api-0.0.1-SNAPSHOT.jar
   ```

| Variable | Default | Meaning |
|---|---|---|
| `PVLENS_DB_URL` | `jdbc:mysql://localhost:3306/pvlens` | JDBC URL |
| `PVLENS_DB_USER` / `PVLENS_DB_PASSWORD` | `pvlens_ro` / empty | Database login (SELECT only) |
| `PVLENS_API_KEYS` | empty | Comma-separated accepted `X-API-Key` values |
| `PVLENS_API_ALLOW_ANONYMOUS` | `false` | Serve without keys. Local development only |
| `PVLENS_DB_POOL_SIZE` | `10` | Connection pool size |
| `PORT` | `8080` | HTTP port |

Interactive docs are at `/swagger-ui.html` and the OpenAPI document at
`/v3/api-docs`. Health checks are at `/actuator/health`. Rate limiting is left
to your API gateway.

## Endpoints

Every `/v1` call needs the header `X-API-Key: <key>`. Lists are paginated with
`page` (from 0) and `size`, and return `{items, page, size, total}`. Errors use
RFC 9457 problem details.

| Endpoint | What it returns |
|---|---|
| `GET /v1/products?name=&ndc=&rxcui=&atc=` | Products matching all given filters. `name` is a case-insensitive substring of an NDC product name or RxNorm term; `atc` is a code or class prefix such as `C09` |
| `GET /v1/products/{id}` | Names, NDCs, RxNorm, ATC, SNOMED ingredients and source SPL labels |
| `GET /v1/products/{id}/adverse-events?boxedWarning=&warning=&exactMatch=&since=` | Labeled adverse events with MedDRA code, PT, first label date, flags and source label GUIDs |
| `GET /v1/products/{id}/indications?exactMatch=` | Labeled indications |
| `GET /v1/products/{id}/label-changes` | FDA Safety Related Label Changes |
| `GET /v1/meddra?q=&tty=&limit=` | MedDRA terms containing `q`, shortest first |
| `GET /v1/meddra/{code}/products?role=ae\|indication&boxedWarning=` | Products listing a MedDRA code. A PT code also matches its LLTs |
| `GET /v1/labels/{guid}` | SPL file metadata and the products it belongs to |
| `GET /v1/labels/{guid}/sections/{adverse-reactions\|indications\|boxed-warning}` | Raw section text, newest label version first |
| `GET /v1/meta` | UMLS version and product count |

Example:

```
curl -H "X-API-Key: $KEY" \
  "http://localhost:8080/v1/meddra/10019851/products?boxedWarning=true"
```

## Notes for clients

- Product IDs are assigned per database build. Store NDC codes, RxCUIs or SPL
  GUIDs if you need identifiers that survive a rebuild.
- When the database is rebuilt, load the new build into a fresh schema and
  point `PVLENS_DB_URL` at it, so clients never see a half-loaded database.

## Tests

`mvn test` loads the real PVLens schema into an in-memory H2 database (MySQL
mode) with a small made-up dataset and exercises every endpoint.
`src/test/resources/schema-h2.sql` is generated from the pipeline schema. If
that schema changes, regenerate it from the repository root with:

```
sed -E '/^(DROP DATABASE|CREATE DATABASE|USE |SET |drop table|#|--)/d' \
  pvlens-db/src/main/resources/sql/create_pvlens_db_structure.sql \
  | cat -s > pvlens-api/src/test/resources/schema-h2.sql
```

(Then put the two header comment lines back at the top.)
