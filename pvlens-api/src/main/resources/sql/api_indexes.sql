-- -----------------------------------------------------------------------
-- Optional indexes for pvlens-api.
--
-- The pipeline schema only indexes primary keys, foreign keys and NDC codes.
-- Run this once after loading a PVLens build so API lookups by MedDRA code,
-- label GUID and vocabulary code don't scan whole tables:
--
--   mysql -u <user> -p pvlens < api_indexes.sql
-- -----------------------------------------------------------------------

USE pvlens;

CREATE INDEX MEDDRA_CODE_IDX      ON MEDDRA (MEDDRA_CODE);
CREATE INDEX MEDDRA_PTCODE_IDX    ON MEDDRA (MEDDRA_PTCODE);
CREATE INDEX MEDDRA_TERM_IDX      ON MEDDRA (MEDDRA_TERM(100));
CREATE INDEX RXNORM_CODE_IDX      ON RXNORM (CODE);
CREATE INDEX ATC_CODE_IDX         ON ATC (CODE);
CREATE INDEX SPL_SRCFILE_GUID_IDX ON SPL_SRCFILE (GUID(64));
CREATE INDEX SPL_AE_TEXT_GUID_IDX  ON SPL_AE_TEXT (GUID(64));
CREATE INDEX SPL_IND_TEXT_GUID_IDX ON SPL_IND_TEXT (GUID(64));
CREATE INDEX SPL_BOX_TEXT_GUID_IDX ON SPL_BOX_TEXT (GUID(64));


-- Read-only account for the API (change the password before use).
-- CREATE USER 'pvlens_ro'@'%' IDENTIFIED BY 'change-me';
-- GRANT SELECT ON pvlens.* TO 'pvlens_ro'@'%';
