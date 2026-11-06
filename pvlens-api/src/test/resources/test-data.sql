-- Small, made-up PVLens dataset for API tests. Codes look real but the rows
-- are not taken from any licensed MedDRA or UMLS release.

INSERT INTO UMLS_VERSION (VERSION) VALUES ('2025A');
INSERT INTO SOURCE_TYPE (ID, SOURCE_TYPE) VALUES (1, 'prescription'), (2, 'otc');

INSERT INTO MEDDRA (ID, MEDDRA_CODE, MEDDRA_PTCODE, MEDDRA_TERM, MEDDRA_TTY, MEDDRA_AUI, MEDDRA_CUI) VALUES
 (1, '10061461', '10061461', 'Erectile dysfunction', 'PT', 'A1', 'C1'),
 (2, '10021515', '10061461', 'Impotence', 'LLT', 'A2', 'C1'),
 (3, '10019851', '10019851', 'Hepatotoxicity', 'PT', 'A3', 'C2'),
 (4, '10004446', '10004446', 'Benign prostatic hyperplasia', 'PT', 'A4', 'C3'),
 (5, '10039906', '10039906', 'Seizure', 'PT', 'A5', 'C4');

INSERT INTO RXNORM (ID, AUI, CUI, CODE, TERM, TTY) VALUES
 (1, 'R1', 'C10', '228790', 'dutasteride', 'IN'),
 (2, 'R2', 'C11', '8691', 'primidone', 'IN');
INSERT INTO ATC (ID, AUI, CUI, CODE, TERM, TTY) VALUES
 (1, 'T1', 'C10', 'G04CB02', 'dutasteride', 'IN'),
 (2, 'T2', 'C11', 'N03AA03', 'primidone', 'IN');
INSERT INTO SNOMED (ID, AUI, CUI, CODE, TERM, TTY) VALUES
 (1, 'S1', 'C10', '385572003', 'Dutasteride', 'PT');

INSERT INTO SUBSTANCE (ID) VALUES (1), (2), (3);

INSERT INTO NDC_CODE (ID, NDC_CODE, PRODUCT_NAME, PRODUCT_NAME_HASH) VALUES
 (1, '0173-0712', 'Avodart', 'h1'),
 (2, '0173-0713', 'Avodart 0.5 mg', 'h2'),
 (3, '0000-0002', 'Primidone 50% tablets', 'h3');
INSERT INTO PRODUCT_NDC (PRODUCT_ID, NDC_ID) VALUES (1, 1), (1, 2), (2, 3);

INSERT INTO SUBSTANCE_RXNORM (PRODUCT_ID, RXNORM_ID) VALUES (1, 1), (2, 2);
INSERT INTO SUBSTANCE_ATC (PRODUCT_ID, NDC_ID, ATC_ID) VALUES (1, 1, 1), (1, 2, 1), (2, 3, 2);
INSERT INTO SUBSTANCE_INGREDIENT (PRODUCT_ID, SNOMED_ID) VALUES (1, 1);

INSERT INTO SPL_SRCFILE (ID, PRODUCT_ID, GUID, XMLFILE_NAME, SOURCE_TYPE_ID, APPLICATION_NUMBER, APPROVAL_DATE, NDA_SPONSOR) VALUES
 (10, 1, 'guid-avodart', 'avodart.xml', 1, 21319, '2001-11-20', 'Example Pharma'),
 (11, 1, 'guid-avodart-2', 'avodart2.xml', 1, 21319, '2001-11-20', 'Example Pharma'),
 (20, 2, 'guid-primidone', 'primidone.xml', 1, 0, NULL, NULL);

INSERT INTO PRODUCT_AE (ID, PRODUCT_ID, MEDDRA_ID, LABEL_DATE, WARNING, BLACKBOX, EXACT_MATCH) VALUES
 (100, 1, 2, '2002-01-01', 0, 0, 1),
 (101, 1, 3, '2010-06-01', 1, 1, 0),
 (102, 2, 3, '2005-03-01', 1, 0, 1),
 (103, 2, 5, '2005-03-01', 0, 0, 1);
INSERT INTO PRODUCT_AE_SRC (AE_ID, SRC_ID) VALUES (100, 10), (100, 11), (101, 11), (102, 20), (103, 20);

INSERT INTO PRODUCT_IND (ID, PRODUCT_ID, MEDDRA_ID, LABEL_DATE, EXACT_MATCH) VALUES
 (200, 1, 4, '2001-11-20', 1);
INSERT INTO PRODUCT_IND_SRC (IND_ID, SRC_ID) VALUES (200, 10);

INSERT INTO SPL_AE_TEXT (GUID, LABEL_DATE, SPL_TEXT) VALUES
 ('guid-avodart', '2002-01-01', 'Older adverse reactions text.'),
 ('guid-avodart', '2010-06-01', 'Impotence and decreased libido were reported.');
INSERT INTO SPL_IND_TEXT (GUID, LABEL_DATE, SPL_TEXT) VALUES
 ('guid-avodart', '2001-11-20', 'Treatment of symptomatic benign prostatic hyperplasia.');

INSERT INTO SRLC (ID, DRUG_ID, APPLICATION_NUMBER, DRUG_NAME, ACTIVE_INGREDIENT, SUPPLEMENT_DATE, DATABASE_UPDATED, URL) VALUES
 (1, 5001, 21319, 'AVODART', 'DUTASTERIDE', '2011-06-14', '2025-10-22', 'https://www.accessdata.fda.gov/scripts/cder/safetylabelingchanges/index.cfm?event=searchdetail.page&DrugNameID=5001');
INSERT INTO SUBSTANCE_SRLC (PRODUCT_ID, DRUG_ID) VALUES (1, 5001);
