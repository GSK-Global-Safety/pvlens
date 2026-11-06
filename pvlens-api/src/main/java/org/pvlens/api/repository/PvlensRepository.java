/*
 * This file is part of PVLens.
 *
 * PVLens is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PVLens is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PVLens.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.pvlens.api.repository;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.pvlens.api.model.Concept;
import org.pvlens.api.model.LabelChange;
import org.pvlens.api.model.LabelSection;
import org.pvlens.api.model.LabelSource;
import org.pvlens.api.model.LabeledTerm;
import org.pvlens.api.model.MeddraTerm;
import org.pvlens.api.model.Ndc;
import org.pvlens.api.model.Page;
import org.pvlens.api.model.ProductDetail;
import org.pvlens.api.model.ProductHit;
import org.pvlens.api.model.ProductSummary;
import org.pvlens.api.model.Section;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Read-only queries against a PVLens database built by {@code pvlens-db}.
 *
 * <p>All user input is bound as a parameter. The only SQL fragments assembled
 * at runtime are fixed strings chosen in this class.</p>
 */
@Repository
public class PvlensRepository {

    /** Product names returned per product in list views. */
    private static final int MAX_NAMES = 5;

    private final JdbcClient jdbc;

    public PvlensRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /* ------------------------------- Products ------------------------------- */

    /** Search filters for {@link #searchProducts}. Null fields are ignored. */
    public record ProductQuery(String name, String ndc, String rxcui, String atc) {
        public boolean isEmpty() {
            return isBlank(name) && isBlank(ndc) && isBlank(rxcui) && isBlank(atc);
        }
    }

    public Page<ProductSummary> searchProducts(ProductQuery q, int page, int size) {
        StringBuilder where = new StringBuilder(" WHERE 1=1");
        Map<String, Object> params = new HashMap<>();

        if (!isBlank(q.name())) {
            where.append("""
                     AND (EXISTS (SELECT 1 FROM PRODUCT_NDC pn JOIN NDC_CODE n ON n.ID = pn.NDC_ID
                                  WHERE pn.PRODUCT_ID = s.ID AND LOWER(n.PRODUCT_NAME) LIKE :name ESCAPE '!')
                       OR EXISTS (SELECT 1 FROM SUBSTANCE_RXNORM sr JOIN RXNORM r ON r.ID = sr.RXNORM_ID
                                  WHERE sr.PRODUCT_ID = s.ID AND LOWER(r.TERM) LIKE :name ESCAPE '!'))
                    """);
            params.put("name", "%" + escapeLike(q.name().trim().toLowerCase(Locale.ROOT)) + "%");
        }
        if (!isBlank(q.ndc())) {
            where.append("""
                     AND EXISTS (SELECT 1 FROM PRODUCT_NDC pn JOIN NDC_CODE n ON n.ID = pn.NDC_ID
                                 WHERE pn.PRODUCT_ID = s.ID AND n.NDC_CODE = :ndc)
                    """);
            params.put("ndc", q.ndc().trim());
        }
        if (!isBlank(q.rxcui())) {
            where.append("""
                     AND EXISTS (SELECT 1 FROM SUBSTANCE_RXNORM sr JOIN RXNORM r ON r.ID = sr.RXNORM_ID
                                 WHERE sr.PRODUCT_ID = s.ID AND r.CODE = :rxcui)
                    """);
            params.put("rxcui", q.rxcui().trim());
        }
        if (!isBlank(q.atc())) {
            // Prefix match so "C09" returns every product in that ATC class.
            where.append("""
                     AND EXISTS (SELECT 1 FROM SUBSTANCE_ATC sa JOIN ATC a ON a.ID = sa.ATC_ID
                                 WHERE sa.PRODUCT_ID = s.ID AND a.CODE LIKE :atc ESCAPE '!')
                    """);
            params.put("atc", escapeLike(q.atc().trim().toUpperCase(Locale.ROOT)) + "%");
        }

        long total = jdbc.sql("SELECT COUNT(*) FROM SUBSTANCE s" + where)
                .params(params).query(Long.class).single();

        List<Long> ids = jdbc.sql("SELECT s.ID FROM SUBSTANCE s" + where
                        + " ORDER BY s.ID LIMIT :limit OFFSET :offset")
                .params(params)
                .param("limit", size)
                .param("offset", (long) page * size)
                .query(Long.class).list();

        Map<Long, List<String>> names = productNames(ids);
        List<ProductSummary> items = ids.stream()
                .map(id -> new ProductSummary(id, limit(names.getOrDefault(id, List.of()), MAX_NAMES)))
                .toList();
        return new Page<>(items, page, size, total);
    }

    public boolean productExists(long id) {
        return jdbc.sql("SELECT COUNT(*) FROM SUBSTANCE WHERE ID = :id")
                .param("id", id).query(Long.class).single() > 0;
    }

    public Optional<ProductDetail> findProduct(long id) {
        if (!productExists(id)) {
            return Optional.empty();
        }

        List<Ndc> ndcs = jdbc.sql("""
                SELECT n.NDC_CODE, n.PRODUCT_NAME FROM PRODUCT_NDC pn
                JOIN NDC_CODE n ON n.ID = pn.NDC_ID
                WHERE pn.PRODUCT_ID = :id ORDER BY n.NDC_CODE
                """).param("id", id)
                .query((rs, i) -> new Ndc(rs.getString(1), rs.getString(2))).list();

        List<Concept> rxnorm = concepts("SUBSTANCE_RXNORM", "RXNORM_ID", "RXNORM", id);
        List<Concept> snomed = concepts("SUBSTANCE_INGREDIENT", "SNOMED_ID", "SNOMED", id);

        // SUBSTANCE_ATC has one row per NDC, so de-duplicate the ATC concepts.
        List<Concept> atc = jdbc.sql("""
                SELECT DISTINCT a.CODE, a.TERM, a.TTY, a.CUI FROM SUBSTANCE_ATC sa
                JOIN ATC a ON a.ID = sa.ATC_ID
                WHERE sa.PRODUCT_ID = :id ORDER BY a.CODE
                """).param("id", id).query(PvlensRepository::concept).list();

        List<LabelSource> labels = jdbc.sql("""
                SELECT f.GUID, f.XMLFILE_NAME, st.SOURCE_TYPE, f.APPLICATION_NUMBER,
                       f.APPROVAL_DATE, f.NDA_SPONSOR
                FROM SPL_SRCFILE f LEFT JOIN SOURCE_TYPE st ON st.ID = f.SOURCE_TYPE_ID
                WHERE f.PRODUCT_ID = :id ORDER BY f.APPROVAL_DATE, f.ID
                """).param("id", id).query(PvlensRepository::labelSource).list();

        List<String> names = productNames(List.of(id)).getOrDefault(id, List.of());
        return Optional.of(new ProductDetail(id, names, ndcs, rxnorm, atc, snomed, labels));
    }

    private List<Concept> concepts(String linkTable, String linkColumn, String vocabTable, long id) {
        // Table and column names are fixed strings from this class, never user input.
        String sql = "SELECT DISTINCT v.CODE, v.TERM, v.TTY, v.CUI FROM " + linkTable + " l"
                + " JOIN " + vocabTable + " v ON v.ID = l." + linkColumn
                + " WHERE l.PRODUCT_ID = :id ORDER BY v.CODE";
        return jdbc.sql(sql).param("id", id).query(PvlensRepository::concept).list();
    }

    /** Distinct NDC product names per product, alphabetical. */
    Map<Long, List<String>> productNames(Collection<Long> productIds) {
        Map<Long, List<String>> out = new HashMap<>();
        if (productIds.isEmpty()) {
            return out;
        }
        jdbc.sql("""
                SELECT DISTINCT pn.PRODUCT_ID, n.PRODUCT_NAME FROM PRODUCT_NDC pn
                JOIN NDC_CODE n ON n.ID = pn.NDC_ID
                WHERE pn.PRODUCT_ID IN (:ids) ORDER BY pn.PRODUCT_ID, n.PRODUCT_NAME
                """).param("ids", productIds)
                .query(rs -> {
                    out.computeIfAbsent(rs.getLong(1), k -> new ArrayList<>()).add(rs.getString(2));
                });
        return out;
    }

    /* ------------------------ Adverse events / indications ------------------------ */

    /** Filters for {@link #adverseEvents}. Null means "don't filter". */
    public record AeFilter(Boolean boxedWarning, Boolean warning, Boolean exactMatch, LocalDate since) {
    }

    public Page<LabeledTerm> adverseEvents(long productId, AeFilter f, int page, int size) {
        StringBuilder where = new StringBuilder(" WHERE ae.PRODUCT_ID = :id");
        Map<String, Object> params = new HashMap<>();
        params.put("id", productId);
        if (f.boxedWarning() != null) {
            where.append(" AND ae.BLACKBOX = :boxed");
            params.put("boxed", f.boxedWarning() ? 1 : 0);
        }
        if (f.warning() != null) {
            where.append(" AND ae.WARNING = :warning");
            params.put("warning", f.warning() ? 1 : 0);
        }
        if (f.exactMatch() != null) {
            where.append(" AND ae.EXACT_MATCH = :exact");
            params.put("exact", f.exactMatch() ? 1 : 0);
        }
        if (f.since() != null) {
            where.append(" AND ae.LABEL_DATE >= :since");
            params.put("since", Date.valueOf(f.since()));
        }
        return labeledTerms("PRODUCT_AE", "PRODUCT_AE_SRC", "AE_ID", true, where.toString(), params, page, size);
    }

    public Page<LabeledTerm> indications(long productId, Boolean exactMatch, int page, int size) {
        StringBuilder where = new StringBuilder(" WHERE ae.PRODUCT_ID = :id");
        Map<String, Object> params = new HashMap<>();
        params.put("id", productId);
        if (exactMatch != null) {
            where.append(" AND ae.EXACT_MATCH = :exact");
            params.put("exact", exactMatch ? 1 : 0);
        }
        return labeledTerms("PRODUCT_IND", "PRODUCT_IND_SRC", "IND_ID", false, where.toString(), params, page, size);
    }

    private Page<LabeledTerm> labeledTerms(String table, String srcTable, String srcFk, boolean isAe,
            String where, Map<String, Object> params, int page, int size) {

        String from = " FROM " + table + " ae JOIN MEDDRA m ON m.ID = ae.MEDDRA_ID";
        long total = jdbc.sql("SELECT COUNT(*)" + from + where).params(params).query(Long.class).single();

        String flags = isAe ? ", ae.WARNING, ae.BLACKBOX" : "";
        Map<Long, LabeledTerm> rows = new LinkedHashMap<>();
        jdbc.sql("SELECT ae.ID, m.MEDDRA_CODE, m.MEDDRA_PTCODE, m.MEDDRA_TERM, m.MEDDRA_TTY,"
                        + " ae.LABEL_DATE, ae.EXACT_MATCH" + flags + from + where
                        + " ORDER BY m.MEDDRA_TERM, ae.ID LIMIT :limit OFFSET :offset")
                .params(params)
                .param("limit", size)
                .param("offset", (long) page * size)
                .query(rs -> {
                    rows.put(rs.getLong("ID"), new LabeledTerm(
                            meddra(rs),
                            localDate(rs, "LABEL_DATE"),
                            isAe ? rs.getBoolean("WARNING") : null,
                            isAe ? rs.getBoolean("BLACKBOX") : null,
                            rs.getBoolean("EXACT_MATCH"),
                            new ArrayList<>()));
                });

        if (!rows.isEmpty()) {
            jdbc.sql("SELECT DISTINCT s." + srcFk + ", f.GUID FROM " + srcTable + " s"
                            + " JOIN SPL_SRCFILE f ON f.ID = s.SRC_ID"
                            + " WHERE s." + srcFk + " IN (:ids) ORDER BY f.GUID")
                    .param("ids", rows.keySet())
                    .query(rs -> {
                        rows.get(rs.getLong(1)).sourceLabelGuids().add(rs.getString(2));
                    });
        }
        return new Page<>(List.copyOf(rows.values()), page, size, total);
    }

    /* --------------------------------- MedDRA --------------------------------- */

    public List<MeddraTerm> searchMeddra(String q, String tty, int limit) {
        String sql = """
                SELECT DISTINCT MEDDRA_CODE, MEDDRA_PTCODE, MEDDRA_TERM, MEDDRA_TTY,
                       CHAR_LENGTH(MEDDRA_TERM) AS LEN
                FROM MEDDRA WHERE LOWER(MEDDRA_TERM) LIKE :q ESCAPE '!'
                """;
        Map<String, Object> params = new HashMap<>();
        params.put("q", "%" + escapeLike(q.trim().toLowerCase(Locale.ROOT)) + "%");
        if (!isBlank(tty)) {
            sql += " AND MEDDRA_TTY = :tty";
            params.put("tty", tty.trim().toUpperCase(Locale.ROOT));
        }
        sql += " ORDER BY LEN, MEDDRA_TERM LIMIT :limit";
        return jdbc.sql(sql).params(params).param("limit", limit)
                .query((rs, i) -> meddra(rs)).list();
    }

    public enum Role { AE, INDICATION }

    /**
     * Products whose label lists a MedDRA code. Matching on the PT code too means
     * a preferred-term code also finds products labelled with its lowest-level terms.
     */
    public Page<ProductHit> productsForMeddra(String code, Role role, Boolean boxedWarning, int page, int size) {
        boolean isAe = role == Role.AE;
        String table = isAe ? "PRODUCT_AE" : "PRODUCT_IND";
        StringBuilder where = new StringBuilder(" WHERE (m.MEDDRA_CODE = :code OR m.MEDDRA_PTCODE = :code)");
        Map<String, Object> params = new HashMap<>();
        params.put("code", code.trim());
        if (isAe && boxedWarning != null) {
            where.append(" AND x.BLACKBOX = :boxed");
            params.put("boxed", boxedWarning ? 1 : 0);
        }
        String from = " FROM " + table + " x JOIN MEDDRA m ON m.ID = x.MEDDRA_ID";

        long total = jdbc.sql("SELECT COUNT(*)" + from + where).params(params).query(Long.class).single();

        String flags = isAe ? ", x.WARNING, x.BLACKBOX" : "";
        List<Object[]> raw = jdbc.sql("SELECT x.PRODUCT_ID, m.MEDDRA_CODE, m.MEDDRA_PTCODE, m.MEDDRA_TERM,"
                        + " m.MEDDRA_TTY, x.LABEL_DATE, x.EXACT_MATCH" + flags + from + where
                        + " ORDER BY x.PRODUCT_ID, x.ID LIMIT :limit OFFSET :offset")
                .params(params)
                .param("limit", size)
                .param("offset", (long) page * size)
                .query((rs, i) -> new Object[] {
                        rs.getLong("PRODUCT_ID"),
                        meddra(rs),
                        localDate(rs, "LABEL_DATE"),
                        isAe ? rs.getBoolean("WARNING") : null,
                        isAe ? rs.getBoolean("BLACKBOX") : null,
                        rs.getBoolean("EXACT_MATCH") })
                .list();

        LinkedHashSet<Long> ids = new LinkedHashSet<>();
        raw.forEach(r -> ids.add((Long) r[0]));
        Map<Long, List<String>> names = productNames(ids);

        List<ProductHit> items = raw.stream().map(r -> new ProductHit(
                (Long) r[0],
                limit(names.getOrDefault((Long) r[0], List.of()), MAX_NAMES),
                (MeddraTerm) r[1],
                (LocalDate) r[2],
                (Boolean) r[3],
                (Boolean) r[4],
                (Boolean) r[5])).toList();
        return new Page<>(items, page, size, total);
    }

    /* --------------------------------- Labels --------------------------------- */

    public List<LabelSource> labelsByGuid(String guid) {
        return jdbc.sql("""
                SELECT f.GUID, f.XMLFILE_NAME, st.SOURCE_TYPE, f.APPLICATION_NUMBER,
                       f.APPROVAL_DATE, f.NDA_SPONSOR
                FROM SPL_SRCFILE f LEFT JOIN SOURCE_TYPE st ON st.ID = f.SOURCE_TYPE_ID
                WHERE f.GUID = :guid ORDER BY f.APPROVAL_DATE, f.ID
                """).param("guid", guid).query(PvlensRepository::labelSource).list();
    }

    public List<Long> productIdsForGuid(String guid) {
        return jdbc.sql("SELECT DISTINCT PRODUCT_ID FROM SPL_SRCFILE WHERE GUID = :guid ORDER BY PRODUCT_ID")
                .param("guid", guid).query(Long.class).list();
    }

    public List<LabelSection> labelSections(String guid, Section section) {
        String sql = "SELECT GUID, LABEL_DATE, SPL_TEXT FROM " + section.table()
                + " WHERE GUID = :guid ORDER BY LABEL_DATE DESC, ID";
        return jdbc.sql(sql).param("guid", guid)
                .query((rs, i) -> new LabelSection(rs.getString("GUID"), section.name(),
                        localDate(rs, "LABEL_DATE"), rs.getString("SPL_TEXT")))
                .list();
    }

    /* ------------------------------ Label changes ------------------------------ */

    public List<LabelChange> labelChanges(long productId) {
        return jdbc.sql("""
                SELECT r.APPLICATION_NUMBER, r.DRUG_NAME, r.ACTIVE_INGREDIENT,
                       r.SUPPLEMENT_DATE, r.DATABASE_UPDATED, r.URL
                FROM SUBSTANCE_SRLC ss JOIN SRLC r ON r.DRUG_ID = ss.DRUG_ID
                WHERE ss.PRODUCT_ID = :id ORDER BY r.SUPPLEMENT_DATE DESC, r.ID
                """).param("id", productId)
                .query((rs, i) -> new LabelChange(
                        rs.getInt("APPLICATION_NUMBER"),
                        rs.getString("DRUG_NAME"),
                        rs.getString("ACTIVE_INGREDIENT"),
                        localDate(rs, "SUPPLEMENT_DATE"),
                        localDate(rs, "DATABASE_UPDATED"),
                        rs.getString("URL")))
                .list();
    }

    /* ---------------------------------- Meta ---------------------------------- */

    public List<String> umlsVersions() {
        return jdbc.sql("SELECT VERSION FROM UMLS_VERSION ORDER BY REF_ID").query(String.class).list();
    }

    public long productCount() {
        return jdbc.sql("SELECT COUNT(*) FROM SUBSTANCE").query(Long.class).single();
    }

    /* --------------------------------- Helpers --------------------------------- */

    private static Concept concept(ResultSet rs, int i) throws SQLException {
        return new Concept(trim(rs.getString(1)), rs.getString(2), trim(rs.getString(3)), rs.getString(4));
    }

    private static LabelSource labelSource(ResultSet rs, int i) throws SQLException {
        int app = rs.getInt("APPLICATION_NUMBER");
        return new LabelSource(
                rs.getString("GUID"),
                rs.getString("XMLFILE_NAME"),
                rs.getString("SOURCE_TYPE"),
                rs.wasNull() || app == 0 ? null : app,
                localDate(rs, "APPROVAL_DATE"),
                rs.getString("NDA_SPONSOR"));
    }

    private static MeddraTerm meddra(ResultSet rs) throws SQLException {
        return new MeddraTerm(
                trim(rs.getString("MEDDRA_CODE")),
                trim(rs.getString("MEDDRA_PTCODE")),
                rs.getString("MEDDRA_TERM"),
                trim(rs.getString("MEDDRA_TTY")));
    }

    private static LocalDate localDate(ResultSet rs, String col) throws SQLException {
        Date d = rs.getDate(col);
        return d == null ? null : d.toLocalDate();
    }

    /** CHAR columns come back space-padded on some drivers. */
    private static String trim(String s) {
        return s == null ? null : s.trim();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    /** Escapes LIKE wildcards using '!' as the escape character. */
    static String escapeLike(String s) {
        return s.replace("!", "!!").replace("%", "!%").replace("_", "!_");
    }

    private static <T> List<T> limit(List<T> list, int max) {
        return list.size() <= max ? list : List.copyOf(list.subList(0, max));
    }
}
