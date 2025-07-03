/****** 
A modified copy of the TDH eCR DataMart SQL Stored proceedure for inputting data into our eCR DataMart.
The code has been modified by removing references to databases within our system. 
Implementation of this code will require modifications to align with your jurisdiction's NBS implementation and datastructure.
*******/
/****** Object:  StoredProcedure SP_eICR_DATAMART    Script Date: xx/xx/xx xx:xx:xx PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/*
v2.0- Stored Procedure for eICR Datamart 
Created by- Hardik Patel & Nathan Williams
*/

CREATE PROCEDURE SP_eICR_DATAMART
AS
BEGIN
    SET NOCOUNT ON;

    -- new variable declaring
    DECLARE @LastRecordStatusTime DATETIME
		, @@ProcessTime DATETIME;

    -- LAST record_status_time
    SELECT @LastRecordStatusTime = MAX(record_status_time)
    FROM eICR_DATAMART 

	-- retrieve current date and time for log timestamps
	SET @@ProcessTime = GETDATE()

    BEGIN TRY
        BEGIN TRANSACTION;

    -- Inserting new records based on record_status_time
    INSERT INTO  eICR_DATAMART (ni.nbs_interface_uid, local_id, derived_doc_id, 
    original_doc_id, condition_code, condition_description, prog_area_cd, jurisdiction_cd, program_jurisdiction_oid,
    pat_first_name, pat_last_name, pat_name_middle, pat_birth_dt, pat_current_sex, pat_marital_status, marital_status_desc, pat_race_code, 
    pat_race, pat_ethnic_code, pat_ethnic, pat_deceased, pat_deceased_date, pat_cell_phone_nbr, pat_home_phone_nbr, pat_email_address, pat_street_addr1, 
    pat_street_addr2, pat_addr_city, pat_addr_county, pat_addr_state, pat_addr_country, pat_addr_zip_code, org_name, org_street_addr1, org_street_addr2, 
    org_addr_city, org_addr_county, org_addr_state, org_addr_country, org_addr_zip_code, org_phone_nbr, external_version_ctrl_nbr, record_status_time, health_care_facility, 
    encounter_type, encounter_code_desc, RECORD_CREATE_DATE)
    SELECT ni.nbs_interface_uid,
        UPPER(doc.local_id) AS local_id,
        mc.MSG_LOCAL_ID AS derived_doc_id,
        REPLACE(ma.ANSWER_TXT, '|', '') AS original_doc_id,
        ctc.condition_cd AS condition_code,
        UPPER(doc.cd_desc_txt) AS condition_description,
        doc.prog_area_cd,
        doc.jurisdiction_cd,
		doc.program_jurisdiction_oid,
        UPPER(mp.PAT_NAME_FIRST_TXT) AS pat_first_name,
        UPPER(mp.PAT_NAME_LAST_TXT) AS pat_last_name,
        UPPER(mp.PAT_NAME_MIDDLE_TXT) AS pat_name_middle,
        CAST(mp.PAT_BIRTH_DT AS DATE) AS pat_birth_dt,
        mp.PAT_CURRENT_SEX_CD AS pat_current_sex,
        mp.PAT_MARITAL_STATUS_CD AS pat_marital_status,
        UPPER(mar.code_desc_txt) AS marital_status_desc,
        CASE
            WHEN mp.PAT_RACE_CATEGORY_CD LIKE '%[0-9][0-9][0-9][0-9]-[0-9]%' THEN mp.PAT_RACE_CATEGORY_CD
            ELSE 'UNKNOWN'
        END AS pat_race_code,
        UPPER(ra.code_desc_txt) AS pat_race,
        CASE
            WHEN mp.PAT_ETHNIC_GROUP_IND_CD LIKE '%[0-9][0-9][0-9][0-9]-[0-9]%' THEN mp.PAT_ETHNIC_GROUP_IND_CD
            ELSE 'UNKNOWN'
        END AS pat_ethnic_code,
        UPPER(grp_eth.code_desc_txt) AS pat_ethnic,
        mp.PAT_DECEASED_IND_CD AS pat_deceased,
        mp.PAT_DECEASED_DT AS pat_deceased_date,
        STUFF(STUFF(STUFF(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(mp.PAT_CELL_PHONE_NBR_TXT, 'tel:', ''), '+1', ''), '(', ''), ')', ''), '-', ''), 1, 0, '('), 5, 0, ')'), 9, 0, '-') AS pat_cell_phone_nbr, --We assume they have a US phone number here
        STUFF(STUFF(STUFF(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(mp.PAT_HOME_PHONE_NBR_TXT, 'tel:', ''), '+1', ''), '(', ''), ')', ''), '-', ''), 1, 0, '('), 5, 0, ')'), 9, 0, '-') AS pat_home_phone_nbr, --We assume they have a US phone number here
        UPPER(mp.PAT_EMAIL_ADDRESS_TXT) AS pat_email_address,
        UPPER(mp.PAT_ADDR_STREET_ADDR1_TXT) AS pat_street_addr1,
        UPPER(mp.PAT_ADDR_STREET_ADDR2_TXT) AS pat_street_addr2,
        UPPER(mp.PAT_ADDR_CITY_TXT) AS pat_addr_city,
        UPPER(mp.PAT_ADDR_COUNTY_CD) AS pat_addr_county,
        UPPER(mp.PAT_ADDR_STATE_CD) AS pat_addr_state,
        CASE
            WHEN mp.PAT_ADDR_COUNTRY_CD = 'US' THEN 'USA'
            ELSE mp.PAT_ADDR_COUNTRY_CD
        END AS pat_addr_country,
        SUBSTRING(mp.PAT_ADDR_ZIP_CODE_TXT, 0, 6) AS pat_addr_zip_code,
        UPPER(mo.ORG_NAME_TXT) AS org_name,
        UPPER(mo.ORG_ADDR_STREET_ADDR1_TXT) AS org_street_addr1,
        UPPER(mo.ORG_ADDR_STREET_ADDR2_TXT) AS org_street_addr2,
        UPPER(mo.ORG_ADDR_CITY_TXT) AS org_addr_city,
        UPPER(mo.ORG_ADDR_COUNTY_CD) AS org_addr_county,
		UPPER(mo.ORG_ADDR_STATE_CD) AS org_addr_state,
        CASE
            WHEN mo.ORG_ADDR_COUNTRY_CD = 'US' THEN 'USA'
            ELSE mo.ORG_ADDR_COUNTRY_CD
        END AS org_addr_country,
        SUBSTRING(mo.ORG_ADDR_ZIP_CODE_TXT, 0, 6) AS org_addr_zip_code,
        STUFF(STUFF(STUFF(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(mo.ORG_PHONE_NBR_TXT, 'tel:', ''), '+1', ''), '(', ''), ')', ''), '-', ''), 1, 0, '('), 5, 0, ')'), 9, 0, '-') AS org_phone_nbr, --We assume they have a US phone number here
        doc.external_version_ctrl_nbr,
        doc.record_status_time,
        CASE
            WHEN facName.ANSWER_TXT IS NOT NULL THEN UPPER(facName.ANSWER_TXT)
            ELSE UPPER(ORG_NAME_TXT)
        END AS health_care_facility,
        EncounterType.Answer_TXT AS encounter_type,
        UPPER(enco.code_desc_txt) AS encounter_code_desc,
		@@ProcessTime AS RECORD_CREATE_DATE
	FROM NBS_interface ni WITH (NOLOCK)
    INNER JOIN NBS_document doc WITH (NOLOCK) ON ni.nbs_interface_uid = doc.nbs_interface_uid
    INNER JOIN MSG_CONTAINER mc WITH (NOLOCK) ON mc.NBS_INTERFACE_UID = ni.nbs_interface_uid
    INNER JOIN MSG_PATIENT mp WITH (NOLOCK) ON mp.MSG_CONTAINER_UID = mc.MSG_CONTAINER_UID
    INNER JOIN MSG_ORGANIZATION mo WITH (NOLOCK) ON mo.MSG_CONTAINER_UID = mc.MSG_CONTAINER_UID
    INNER JOIN MSG_CASE_INVESTIGATION mci WITH (NOLOCK) ON mci.MSG_CONTAINER_UID = mc.MSG_CONTAINER_UID
    LEFT JOIN MSG_ANSWER ma WITH (NOLOCK) ON ma.MSG_CONTAINER_UID = mc.MSG_CONTAINER_UID
        AND ma.QUESTION_IDENTIFIER = 'Original Global Unique ID'
    LEFT JOIN MSG_ANSWER facName WITH (NOLOCK) ON facName.MSG_CONTAINER_UID = mc.MSG_CONTAINER_UID
        AND facName.QUESTION_IDENTIFIER = 'Health Care Facility Name'
    LEFT JOIN MSG_ANSWER EncounterType WITH (NOLOCK) ON EncounterType.MSG_CONTAINER_UID = mc.MSG_CONTAINER_UID
        AND EncounterType.QUESTION_IDENTIFIER = 'Encounter Type'
    INNER JOIN Code_to_condition ctc WITH (NOLOCK) ON 
        CASE 
            WHEN CHARINDEX('^', mci.INV_CONDITION_CD) > 0 
            THEN SUBSTRING(mci.INV_CONDITION_CD, 1, CHARINDEX('^', mci.INV_CONDITION_CD) - 1)
            ELSE NULL
        END = ctc.code
    LEFT JOIN Race_code ra WITH (NOLOCK) ON mp.PAT_RACE_CATEGORY_CD = ra.code
    LEFT JOIN Code_value_general grp_eth WITH (NOLOCK) ON mp.PAT_ETHNIC_GROUP_IND_CD = grp_eth.code
        AND grp_eth.code_set_nm = 'P_ETHN_GRP'
    LEFT JOIN Code_value_general mar WITH (NOLOCK) ON mp.PAT_MARITAL_STATUS_CD = mar.code 
        AND mar.code_set_nm = 'P_MARITAL'
    LEFT JOIN Code_value_general enco WITH (NOLOCK) ON EncounterType.Answer_TXT = enco.code 
        AND enco.code_set_nm = 'ACT_ENCOUNTER_CODE'
        ---- Added where condition to refelect only updated records
        --also if time is same- we will make sure that nbs_interface_uid stays unique all time. 
    WHERE doc.record_status_time > @LastRecordStatusTime
    AND NOT EXISTS (
            SELECT 1
            FROM eICR_DATAMART
            WHERE ni.nbs_interface_uid = eICR_DATAMART.nbs_interface_uid
        )
		
		--update datamart log table with batch count and timestamp
		INSERT INTO [dbo].[DATAMART_REFRESH_ACTIVITY_LOG]
           ([STORED_PROCEDURE_NM]
           ,[DATAMART_NM]
           ,[DATAMART_ROW_COUNT]
           ,[REFRESH_TIME])
        SELECT
           'SP_eICR_DATAMART'
           , 'eICR_DATAMART'
           , (SELECT COUNT(*) FROM eICR_DATAMART WHERE RECORD_CREATE_DATE = @@ProcessTime)
		   , @@ProcessTime
		
		;

    COMMIT TRANSACTION;
END TRY
    
    --rollback if error occurs
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;
        
        ---catch the basic errors-
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;

---end of the sp!!! 
GO

