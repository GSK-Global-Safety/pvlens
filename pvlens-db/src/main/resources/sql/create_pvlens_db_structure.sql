-- Drop the existing database if it exists
DROP DATABASE IF EXISTS pvlens;
CREATE DATABASE pvlens;

USE pvlens;

-- -----------------------------------------------------------------------
-- mysql SQL script for schema pvlens
-- -----------------------------------------------------------------------

SET FOREIGN_KEY_CHECKS = 0;

drop table if exists UMLS_VERSION;
drop table if exists MEDDRA;
drop table if exists RXNORM;
drop table if exists SNOMED;
drop table if exists ATC;
drop table if exists SUBSTANCE;
drop table if exists SOURCE_TYPE;
drop table if exists SPL_SRCFILE;
drop table if exists SRLC;
drop table if exists SUBSTANCE_SRLC;
drop table if exists SUBSTANCE_ATC;
drop table if exists SUBSTANCE_RXNORM;
drop table if exists SUBSTANCE_SNOMED_PT;
drop table if exists SUBSTANCE_SNOMED_PARENT;
drop table if exists SUBSTANCE_INGREDIENT;
drop table if exists NDC_CODE;
drop table if exists PRODUCT_NDC;
drop table if exists PRODUCT_IND;
drop table if exists PRODUCT_AE;
drop table if exists PRODUCT_AE_SRC;
drop table if exists PRODUCT_IND_SRC;
drop table if exists SPL_AE_TEXT;
drop table if exists SPL_IND_TEXT;
drop table if exists SPL_BOX_TEXT;

# -----------------------------------------------------------------------
# UMLS_VERSION
# -----------------------------------------------------------------------
CREATE TABLE UMLS_VERSION
(
    REF_ID INTEGER NOT NULL AUTO_INCREMENT,
    VERSION CHAR(5),
    PRIMARY KEY(REF_ID)
);


# -----------------------------------------------------------------------
# MEDDRA
# -----------------------------------------------------------------------
CREATE TABLE MEDDRA
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    MEDDRA_CODE CHAR(12),
    MEDDRA_PTCODE CHAR(12),
    MEDDRA_TERM VARCHAR(500),
    MEDDRA_TTY CHAR(10),
    MEDDRA_AUI VARCHAR(15),
    MEDDRA_CUI VARCHAR(15),
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# RXNORM
# -----------------------------------------------------------------------
CREATE TABLE RXNORM
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    AUI VARCHAR(15),
    CUI VARCHAR(15),
    CODE CHAR(12),
    TERM VARCHAR(2500),
    TTY CHAR(10),
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# SNOMED
# -----------------------------------------------------------------------
CREATE TABLE SNOMED
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    AUI VARCHAR(15),
    CUI VARCHAR(15),
    CODE CHAR(20),
    TERM VARCHAR(2500),
    TTY CHAR(10),
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# ATC
# -----------------------------------------------------------------------
CREATE TABLE ATC
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    AUI VARCHAR(15),
    CUI VARCHAR(15),
    CODE CHAR(20),
    TERM VARCHAR(2500),
    TTY CHAR(10),
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# SUBSTANCE
# -----------------------------------------------------------------------
CREATE TABLE SUBSTANCE
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# SOURCE_TYPE
# -----------------------------------------------------------------------
CREATE TABLE SOURCE_TYPE
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    SOURCE_TYPE VARCHAR(50),
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# SPL_SRCFILE
# -----------------------------------------------------------------------
CREATE TABLE SPL_SRCFILE
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    PRODUCT_ID INTEGER NOT NULL,
    GUID VARCHAR(500),
    XMLFILE_NAME VARCHAR(500),
    SOURCE_TYPE_ID INTEGER,
    APPLICATION_NUMBER INTEGER default 0,
    APPROVAL_DATE DATE,
    NDA_SPONSOR VARCHAR(200),
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# SRLC
# -----------------------------------------------------------------------
CREATE TABLE SRLC
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    DRUG_ID INTEGER NOT NULL,
    APPLICATION_NUMBER INTEGER NOT NULL,
    DRUG_NAME VARCHAR(500),
    ACTIVE_INGREDIENT VARCHAR(500),
    SUPPLEMENT_DATE DATE,
    DATABASE_UPDATED DATE,
    URL VARCHAR(500),
    PRIMARY KEY(ID),
    UNIQUE SRLC_UQ_1 (DRUG_ID)
);


# -----------------------------------------------------------------------
# SUBSTANCE_SRLC
# -----------------------------------------------------------------------
CREATE TABLE SUBSTANCE_SRLC
(
    PRODUCT_ID INTEGER,
    DRUG_ID INTEGER
);


# -----------------------------------------------------------------------
# SUBSTANCE_ATC
# -----------------------------------------------------------------------
CREATE TABLE SUBSTANCE_ATC
(
    PRODUCT_ID INTEGER,
    NDC_ID INTEGER,
    ATC_ID INTEGER
);


# -----------------------------------------------------------------------
# SUBSTANCE_RXNORM
# -----------------------------------------------------------------------
CREATE TABLE SUBSTANCE_RXNORM
(
    PRODUCT_ID INTEGER,
    RXNORM_ID INTEGER
);


# -----------------------------------------------------------------------
# SUBSTANCE_SNOMED_PT
# -----------------------------------------------------------------------
CREATE TABLE SUBSTANCE_SNOMED_PT
(
    PRODUCT_ID INTEGER,
    SNOMED_ID INTEGER
);


# -----------------------------------------------------------------------
# SUBSTANCE_SNOMED_PARENT
# -----------------------------------------------------------------------
CREATE TABLE SUBSTANCE_SNOMED_PARENT
(
    PRODUCT_ID INTEGER,
    SNOMED_ID INTEGER
);


# -----------------------------------------------------------------------
# SUBSTANCE_INGREDIENT
# -----------------------------------------------------------------------
CREATE TABLE SUBSTANCE_INGREDIENT
(
    PRODUCT_ID INTEGER,
    SNOMED_ID INTEGER
);


# -----------------------------------------------------------------------
# NDC_CODE
# -----------------------------------------------------------------------
CREATE TABLE NDC_CODE
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    NDC_CODE VARCHAR(50) NOT NULL,
    PRODUCT_NAME VARCHAR(3500) NOT NULL,
    PRODUCT_NAME_HASH CHAR(64) NOT NULL,
    PRIMARY KEY(ID),
    UNIQUE NDC_CODE_PRODUCT_NAME_UQ (NDC_CODE, PRODUCT_NAME_HASH),    INDEX NDC_CODE_IDX(NDC_CODE)
);


# -----------------------------------------------------------------------
# PRODUCT_NDC
# -----------------------------------------------------------------------
CREATE TABLE PRODUCT_NDC
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    PRODUCT_ID INTEGER NOT NULL,
    NDC_ID INTEGER NOT NULL,
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# PRODUCT_IND
# -----------------------------------------------------------------------
CREATE TABLE PRODUCT_IND
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    PRODUCT_ID INTEGER NOT NULL,
    MEDDRA_ID INTEGER NOT NULL,
    LABEL_DATE DATE,
    EXACT_MATCH TINYINT(1),
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# PRODUCT_AE
# -----------------------------------------------------------------------
CREATE TABLE PRODUCT_AE
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    PRODUCT_ID INTEGER NOT NULL,
    MEDDRA_ID INTEGER NOT NULL,
    LABEL_DATE DATE,
    WARNING TINYINT(1),
    BLACKBOX TINYINT(1),
    EXACT_MATCH TINYINT(1),
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# PRODUCT_AE_SRC
# -----------------------------------------------------------------------
CREATE TABLE PRODUCT_AE_SRC
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    AE_ID INTEGER NOT NULL,
    SRC_ID INTEGER NOT NULL,
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# PRODUCT_IND_SRC
# -----------------------------------------------------------------------
CREATE TABLE PRODUCT_IND_SRC
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    IND_ID INTEGER NOT NULL,
    SRC_ID INTEGER NOT NULL,
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# SPL_AE_TEXT
# -----------------------------------------------------------------------
CREATE TABLE SPL_AE_TEXT
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    GUID VARCHAR(500) NOT NULL,
    LABEL_DATE DATE,
    SPL_TEXT VARCHAR(15800) NOT NULL,
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# SPL_IND_TEXT
# -----------------------------------------------------------------------
CREATE TABLE SPL_IND_TEXT
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    GUID VARCHAR(500) NOT NULL,
    LABEL_DATE DATE,
    SPL_TEXT VARCHAR(15800) NOT NULL,
    PRIMARY KEY(ID)
);


# -----------------------------------------------------------------------
# SPL_BOX_TEXT
# -----------------------------------------------------------------------
CREATE TABLE SPL_BOX_TEXT
(
    ID INTEGER NOT NULL AUTO_INCREMENT,
    GUID VARCHAR(500) NOT NULL,
    LABEL_DATE DATE,
    SPL_TEXT VARCHAR(15800) NOT NULL,
    PRIMARY KEY(ID)
);

ALTER TABLE SPL_SRCFILE
    ADD CONSTRAINT SPL_SRCFILE_FK_1
    FOREIGN KEY (SOURCE_TYPE_ID)
    REFERENCES SOURCE_TYPE (ID);

ALTER TABLE SPL_SRCFILE
    ADD CONSTRAINT SPL_SRCFILE_FK_2
    FOREIGN KEY (PRODUCT_ID)
    REFERENCES SUBSTANCE (ID);

ALTER TABLE SUBSTANCE_SRLC
    ADD CONSTRAINT SUBSTANCE_SRLC_FK_1
    FOREIGN KEY (PRODUCT_ID)
    REFERENCES SUBSTANCE (ID);

ALTER TABLE SUBSTANCE_SRLC
    ADD CONSTRAINT SUBSTANCE_SRLC_FK_2
    FOREIGN KEY (DRUG_ID)
    REFERENCES SRLC (DRUG_ID);

ALTER TABLE SUBSTANCE_ATC
    ADD CONSTRAINT SUBSTANCE_ATC_FK_1
    FOREIGN KEY (PRODUCT_ID)
    REFERENCES SUBSTANCE (ID);

ALTER TABLE SUBSTANCE_ATC
    ADD CONSTRAINT SUBSTANCE_ATC_FK_2
    FOREIGN KEY (NDC_ID)
    REFERENCES NDC_CODE (ID);

ALTER TABLE SUBSTANCE_ATC
    ADD CONSTRAINT SUBSTANCE_ATC_FK_3
    FOREIGN KEY (ATC_ID)
    REFERENCES ATC (ID);

ALTER TABLE SUBSTANCE_RXNORM
    ADD CONSTRAINT SUBSTANCE_RXNORM_FK_1
    FOREIGN KEY (PRODUCT_ID)
    REFERENCES SUBSTANCE (ID);

ALTER TABLE SUBSTANCE_RXNORM
    ADD CONSTRAINT SUBSTANCE_RXNORM_FK_2
    FOREIGN KEY (RXNORM_ID)
    REFERENCES RXNORM (ID);

ALTER TABLE SUBSTANCE_SNOMED_PT
    ADD CONSTRAINT SUBSTANCE_SNOMED_PT_FK_1
    FOREIGN KEY (PRODUCT_ID)
    REFERENCES SUBSTANCE (ID);

ALTER TABLE SUBSTANCE_SNOMED_PT
    ADD CONSTRAINT SUBSTANCE_SNOMED_PT_FK_2
    FOREIGN KEY (SNOMED_ID)
    REFERENCES SNOMED (ID);

ALTER TABLE SUBSTANCE_SNOMED_PARENT
    ADD CONSTRAINT SUBSTANCE_SNOMED_PARENT_FK_1
    FOREIGN KEY (PRODUCT_ID)
    REFERENCES SUBSTANCE (ID);

ALTER TABLE SUBSTANCE_SNOMED_PARENT
    ADD CONSTRAINT SUBSTANCE_SNOMED_PARENT_FK_2
    FOREIGN KEY (SNOMED_ID)
    REFERENCES SNOMED (ID);

ALTER TABLE SUBSTANCE_INGREDIENT
    ADD CONSTRAINT SUBSTANCE_INGREDIENT_FK_1
    FOREIGN KEY (PRODUCT_ID)
    REFERENCES SUBSTANCE (ID);

ALTER TABLE SUBSTANCE_INGREDIENT
    ADD CONSTRAINT SUBSTANCE_INGREDIENT_FK_2
    FOREIGN KEY (SNOMED_ID)
    REFERENCES SNOMED (ID);

ALTER TABLE PRODUCT_NDC
    ADD CONSTRAINT PRODUCT_NDC_FK_1
    FOREIGN KEY (PRODUCT_ID)
    REFERENCES SUBSTANCE (ID);

ALTER TABLE PRODUCT_NDC
    ADD CONSTRAINT PRODUCT_NDC_FK_2
    FOREIGN KEY (NDC_ID)
    REFERENCES NDC_CODE (ID);

ALTER TABLE PRODUCT_IND
    ADD CONSTRAINT PRODUCT_IND_FK_1
    FOREIGN KEY (PRODUCT_ID)
    REFERENCES SUBSTANCE (ID);

ALTER TABLE PRODUCT_IND
    ADD CONSTRAINT PRODUCT_IND_FK_2
    FOREIGN KEY (MEDDRA_ID)
    REFERENCES MEDDRA (ID);

ALTER TABLE PRODUCT_AE
    ADD CONSTRAINT PRODUCT_AE_FK_1
    FOREIGN KEY (PRODUCT_ID)
    REFERENCES SUBSTANCE (ID);

ALTER TABLE PRODUCT_AE
    ADD CONSTRAINT PRODUCT_AE_FK_2
    FOREIGN KEY (MEDDRA_ID)
    REFERENCES MEDDRA (ID);

ALTER TABLE PRODUCT_AE_SRC
    ADD CONSTRAINT PRODUCT_AE_SRC_FK_1
    FOREIGN KEY (AE_ID)
    REFERENCES PRODUCT_AE (ID);

ALTER TABLE PRODUCT_AE_SRC
    ADD CONSTRAINT PRODUCT_AE_SRC_FK_2
    FOREIGN KEY (SRC_ID)
    REFERENCES SPL_SRCFILE (ID);

ALTER TABLE PRODUCT_IND_SRC
    ADD CONSTRAINT PRODUCT_IND_SRC_FK_1
    FOREIGN KEY (IND_ID)
    REFERENCES PRODUCT_IND (ID);

ALTER TABLE PRODUCT_IND_SRC
    ADD CONSTRAINT PRODUCT_IND_SRC_FK_2
    FOREIGN KEY (SRC_ID)
    REFERENCES SPL_SRCFILE (ID);



