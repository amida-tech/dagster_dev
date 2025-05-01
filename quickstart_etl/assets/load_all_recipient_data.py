import os
from dagster import asset, Output
from snowflake.snowpark import Session
from quickstart_etl.assets.convert_csv_to_parquet import convert_csv_to_parquet
from io import BytesIO

SNOWFLAKE_USR = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PWD = os.environ["SNOWFLAKE_PASSWORD"]

print(" Snowflake User:", os.getenv("SNOWFLAKE_USER"))
print(" Snowflake Password Present:", "SNOWFLAKE_PASSWORD" in os.environ)


@asset(deps=["convert_csv_to_parquet"])
def load_all_recipient_data():
    session = Session.builder.configs({
        "account": "vba67968.east-us-2.azure",
        "user": SNOWFLAKE_USR,
        "password": SNOWFLAKE_PWD,
         "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "ADW_DEV",
        "schema": "RAW"
    }).create()

    sql_statements = [
        """
            COPY INTO RAW.B_ADDR_TB
        ( ETL_DATA_SOURCE,
        ETL_BATCH_ID,
        DATE_INSERT_ROW,
        B_SYS_ID,
        R_PAYR_ID,
        B_ADDR_TY_CD,
        B_ADDR_BEG_DT,
        B_ADDR_END_DT,
        B_ICO_NAM,
        B_ADDR_1,
        B_ADDR_2,
        B_CITY,
        B_GEO_CNTY_CD,
        B_PSTL_CD,
        B_ST_CD,
        B_CNTRY_CD,
        B_PRIM_PHON_NUM,
        B_DAY_PHON_NUM,
        B_EVNG_PHON_NUM,
        B_GRDN_PHON_NUM,
        G_AUD_ADD_USER_ID,
        G_AUD_ADD_TS,
        G_AUD_USER_ID,
        G_AUD_TS,
        B_CMNT_TXT)
        
        FROM (
        SELECT
        'RECIPIENT_DAGSTER' AS ETL_DATA_SOURCE,
            1 AS ETL_BATCH_ID,
        CURRENT_TIMESTAMP AS DATE_INSERT_ROW,
            t.$1:"b_sys_id"::INT,
            t.$1:"r_payr_id"::INT,
            t.$1:"b_addr_ty_cd"::STRING,
            t.$1:"b_addr_beg_dt"::DATE,
            t.$1:"b_addr_end_dt"::DATE,
            t.$1:"b_ico_nam"::STRING,
            t.$1:"b_addr_1"::STRING,
            t.$1:"b_addr_2"::STRING,
            t.$1:"b_city"::STRING,
            t.$1:"b_geo_cnty_cd"::STRING,
            t.$1:"b_pstl_cd"::STRING,
            t.$1:"b_st_cd"::STRING,
            t.$1:"b_cntry_cd"::STRING,
            t.$1:"b_prim_phon_num"::STRING,
            t.$1:"b_day_phon_num"::STRING,
            t.$1:"b_evng_phon_num"::STRING,
            t.$1:"b_grdn_phon_num"::STRING,
            t.$1:"g_aud_add_user_id"::STRING,
            t.$1:"g_aud_add_ts"::TIMESTAMP,
            t.$1:"g_aud_user_id"::STRING,
            t.$1:"g_aud_ts"::TIMESTAMP,
            t.$1:"b_cmnt_txt"::STRING
        FROM @extstage_recipient_dagster/b_addr_tb.parquet (FILE_FORMAT => parquet_format) t
        )
        FILE_FORMAT = parquet_format
        LOAD_MODE = FULL_INGEST;
        """,
        """
        -- COPY INTO for B_AUTH_REP_TB
        COPY INTO RAW.B_AUTH_REP_TB (
        ETL_DATA_SOURCE,
        ETL_BATCH_ID,
        DATE_INSERT_ROW,
        B_SYS_ID,
        B_AUTH_REP_BEG_DT,
        B_AUTH_REP_REC_TY_CD,
        B_AUTH_REP_END_DT,
        B_AUTH_REP_LANG,
        B_AUTH_REP_ZIP,
        B_AUTH_REP_ST,
        B_AUTH_REP_CITY,
        B_AUTH_REP_ADDR2,
        B_AUTH_REP_ADDR1,
        B_AUTH_REP_SUFIX,
        B_AUTH_REP_MI,
        B_AUTH_REP_LNAME,
        B_AUTH_REP_FNAME,
        B_AUTH_REP_REL_IND,
        G_AUD_ADD_USER_ID,
        G_AUD_ADD_TS,
        G_AUD_USER_ID,
        G_AUD_TS,
        R_PAYER_ID,
        B_AUTH_REP_PHN_NUM
        )
        FROM (
        SELECT
            'RECIPIENT_DAGSTER' AS ETL_DATA_SOURCE,
            1 AS ETL_BATCH_ID,
            CURRENT_TIMESTAMP AS DATE_INSERT_ROW,
            t.$1:"b_sys_id"::INT,
            t.$1:"b_auth_rep_beg_dt"::DATE,
            t.$1:"b_auth_rep_rec_ty_cd"::STRING,
            t.$1:"b_auth_rep_end_dt"::DATE,
            t.$1:"b_auth_rep_lang"::STRING,
            t.$1:"b_auth_rep_zip"::STRING,
            t.$1:"b_auth_rep_st"::STRING,
            t.$1:"b_auth_rep_city"::STRING,
            t.$1:"b_auth_rep_addr2"::STRING,
            t.$1:"b_auth_rep_addr1"::STRING,
            t.$1:"b_auth_rep_sufix"::STRING,
            t.$1:"b_auth_rep_mi"::STRING,
            t.$1:"b_auth_rep_lname"::STRING,
            t.$1:"b_auth_rep_fname"::STRING,
            t.$1:"b_auth_rep_rel_ind"::STRING,
            t.$1:"g_aud_add_user_id"::STRING,
            t.$1:"g_aud_add_ts"::TIMESTAMP,
            t.$1:"g_aud_user_id"::STRING,
            t.$1:"g_aud_ts"::TIMESTAMP,
            t.$1:"r_payer_id"::NUMBER,
            t.$1:"b_auth_rep_phn_num"::STRING
        FROM @extstage_recipient_dagster/b_auth_rep_tb.parquet (FILE_FORMAT => parquet_format) t
        )
        FILE_FORMAT = parquet_format
        LOAD_MODE = FULL_INGEST;
        """,
        """
        
        
        COPY INTO RAW.B_DETAIL_TB (
        ETL_DATA_SOURCE,
        ETL_BATCH_ID,
        DATE_INSERT_ROW,
        B_SYS_ID,
        B_LAST_NAM,
        B_FST_NAM,
        B_MI_NAM,
        B_SFX_NAM,
        B_LAST_NAM_SNDX,
        B_FST_NAM_SNDX,
        B_GENDER_CD,
        B_SSN_NUM,
        B_ETH_CD,
        B_REFG_CD,
        B_REFG_ENTRY_DT,
        B_FAM_STAT_CD,
        B_DOB_DT,
        B_DMA_INDV_STAT_CD,
        B_DMH_INDV_STAT_CD,
        B_DOD_DT,
        B_REL_TO_PAYE_CD,
        B_WRK_HIST_IND,
        B_JOB_RGSTR_IND,
        B_HSVCS_COMP_IND,
        B_FST_EMP_IND,
        B_FST_EMP_DT,
        B_GROSS_AMT,
        B_ADULT_CARE_AMT,
        B_WRK_EXP_AMT,
        B_NET_AMT,
        B_RSDI_CLM_NUM,
        B_IVD_IND,
        B_SPECL_RPT_CD,
        B_SPECL_ND_CD,
        T_TPL_RCVRY_IND,
        B_LV_ARRANGE_CD,
        B_CS_ID_NUM,
        B_REC_LAST_UPDT_DT,
        B_CS_TERM_DT,
        B_REC_CREATE_DT,
        B_FLOOD_IND,
        B_P_LAST_CHG_DT,
        B_CITZ_CD,
        B_CITZ_CD_UPDT_DT,
        B_PACE_IND,
        B_ALIEN_ID_NUM,
        T_EST_RCVRY_IND,
        G_AUD_ADD_USER_ID,
        G_AUD_ADD_TS,
        G_AUD_USER_ID,
        G_AUD_TS,
        B_CARD_ISSUE_DATE,
        B_LTST_CNTY_CD,
        B_TRNS_GNDR_IND
        )
        FROM (
        SELECT
            'RECIPIENT_DAGSTER' AS ETL_DATA_SOURCE,
            1 AS ETL_BATCH_ID,
            CURRENT_TIMESTAMP AS DATE_INSERT_ROW,
            t.$1:"b_sys_id"::INT,
            t.$1:"b_last_nam"::STRING,
            t.$1:"b_fst_nam"::STRING,
            t.$1:"b_mi_nam"::STRING,
            t.$1:"b_sfx_nam"::STRING,
            t.$1:"b_last_nam_sndx"::STRING,
            t.$1:"b_fst_nam_sndx"::STRING,
            t.$1:"b_gender_cd"::STRING,
            t.$1:"b_ssn_num"::STRING,
            t.$1:"b_eth_cd"::STRING,
            t.$1:"b_refg_cd"::STRING,
            t.$1:"b_refg_entry_dt"::DATE,
            t.$1:"b_fam_stat_cd"::STRING,
            t.$1:"b_dob_dt"::DATE,
            t.$1:"b_dma_indv_stat_cd"::STRING,
            t.$1:"b_dmh_indv_stat_cd"::STRING,
            t.$1:"b_dod_dt"::DATE,
            t.$1:"b_rel_to_paye_cd"::STRING,
            t.$1:"b_wrk_hist_ind"::STRING,
            t.$1:"b_job_rgstr_ind"::STRING,
            t.$1:"b_hsvcs_comp_ind"::STRING,
            t.$1:"b_fst_emp_ind"::STRING,
            t.$1:"b_fst_emp_dt"::DATE,
            t.$1:"b_gross_amt"::DECIMAL(9,2),
            t.$1:"b_adult_care_amt"::DECIMAL(9,2),
            t.$1:"b_wrk_exp_amt"::DECIMAL(9,2),
            t.$1:"b_net_amt"::DECIMAL(9,2),
            t.$1:"b_rsdi_clm_num"::STRING,
            t.$1:"b_ivd_ind"::STRING,
            t.$1:"b_specl_rpt_cd"::STRING,
            t.$1:"b_specl_nd_cd"::STRING,
            t.$1:"t_tpl_rcvry_ind"::STRING,
            t.$1:"b_lv_arrange_cd"::STRING,
            t.$1:"b_cs_id_num"::INT,
            t.$1:"b_rec_last_updt_dt"::DATE,
            t.$1:"b_cs_term_dt"::DATE,
            t.$1:"b_rec_create_dt"::STRING,
            t.$1:"b_flood_ind"::STRING,
            t.$1:"b_p_last_chg_dt"::DATE,
            t.$1:"b_citz_cd"::STRING,
            t.$1:"b_citz_cd_updt_dt"::DATE,
            t.$1:"b_pace_ind"::STRING,
            t.$1:"b_alien_id_num"::STRING,
            t.$1:"t_est_rcvry_ind"::STRING,
            t.$1:"g_aud_add_user_id"::STRING,
            t.$1:"g_aud_add_ts"::TIMESTAMP,
            t.$1:"g_aud_user_id"::STRING,
            t.$1:"g_aud_ts"::TIMESTAMP,
            t.$1:"b_card_issue_date"::DATE,
            t.$1:"b_ltst_cnty_cd"::STRING,
            t.$1:"b_trns_gndr_ind"::STRING
        FROM @extstage_recipient_dagster/b_detail_tb.parquet (FILE_FORMAT => parquet_format) t
        )
        FILE_FORMAT = parquet_format
        LOAD_MODE = FULL_INGEST;
        """,
        """
        COPY INTO RAW.B_ELIG_SPN_TB (
        ETL_DATA_SOURCE,
        ETL_BATCH_ID,
        DATE_INSERT_ROW,
        B_SYS_ID,
        B_ELIG_ID_NUM,
        B_ELIG_HIST_FROM_DT,
        B_ELIG_AUTH_BEG_DT,
        B_ELIG_HIST_END_DT,
        B_ST_AUTH_CD,
        B_AID_PROG_CD,
        B_AID_CAT_CD,
        B_MCAID_CLS_CD,
        B_SSI_STAT_CD,
        B_SPECL_COV_CD,
        B_CS_ID_NUM,
        B_LV_ARRANGE_CD,
        B_AMBL_CPCTY_CD,
        B_ELIG_COV_CD,
        B_LIAB_AMT,
        B_DED_BAL_LIAB_TY_CD,
        B_PYMT_TY_CD,
        B_DIST_CD,
        B_PCP_AUTO_ASGN_CD,
        B_PCP_CHG_RSN_CD,
        B_PIED_CD,
        B_REC_STAT_CD,
        B_LME_SYS_ID,
        B_LME_NPI_ID,
        B_PCP_ID,
        B_SPROG_CD_1,
        B_SPROG_CD_2,
        B_SPROG_CD_3,
        B_SPROG_CD_4,
        B_APP_PVRTY_LVL_CD_1,
        B_APP_PVRTY_LVL_CD_2,
        B_APP_PVRTY_LVL_CD_3,
        B_APP_PVRTY_LVL_CD_4,
        B_SPECL_ND_CD,
        P_NPI_NUM,
        P_LOCATOR_CD,
        B_FUND_SPLIT_CD,
        B_REG_CD,
        G_AUD_ADD_USER_ID,
        G_AUD_ADD_TS,
        G_AUD_USER_ID,
        G_AUD_TS,
        B_CA_EXMT_CD,
        R_PAYR_ID,
        R_HLTH_PLN_ID,
        B_ENRL_FEE_DT,
        B_ADMIN_CNTY_CD,
        B_INC_AFT_TAX_AMT,
        B_GROSS_INC_AFT_TAX_AMT,
        B_GROSS_INC_AMT,
        B_TOT_FML_CNT,
        B_FAM_CHLD_CNT,
        B_FAM_ADULT_CNT
        )
        FROM (
        SELECT
            'RECIPIENT_DAGSTER' AS ETL_DATA_SOURCE,
            1 AS ETL_BATCH_ID,
            CURRENT_TIMESTAMP AS DATE_INSERT_ROW,
            t.$1:"b_sys_id"::INT,
            t.$1:"b_elig_id_num"::INT,
            t.$1:"b_elig_hist_from_dt"::DATE,
            t.$1:"b_elig_auth_beg_dt"::DATE,
            t.$1:"b_elig_hist_end_dt"::DATE,
            t.$1:"b_st_auth_cd"::STRING,
            t.$1:"b_aid_prog_cd"::STRING,
            t.$1:"b_aid_cat_cd"::STRING,
            t.$1:"b_mcaid_cls_cd"::STRING,
            t.$1:"b_ssi_stat_cd"::STRING,
            t.$1:"b_specl_cov_cd"::STRING,
            t.$1:"b_cs_id_num"::INT,
            t.$1:"b_lv_arrange_cd"::STRING,
            t.$1:"b_ambl_cpcty_cd"::STRING,
            t.$1:"b_elig_cov_cd"::STRING,
            t.$1:"b_liab_amt"::DECIMAL(9,2),
            t.$1:"b_ded_bal_liab_ty_cd"::STRING,
            t.$1:"b_pymt_ty_cd"::STRING,
            t.$1:"b_dist_cd"::STRING,
            t.$1:"b_pcp_auto_asgn_cd"::STRING,
            t.$1:"b_pcp_chg_rsn_cd"::STRING,
            t.$1:"b_pied_cd"::STRING,
            t.$1:"b_rec_stat_cd"::STRING,
            t.$1:"b_lme_sys_id"::STRING,
            t.$1:"b_lme_npi_id"::STRING,
            t.$1:"b_pcp_id"::STRING,
            t.$1:"b_sprog_cd_1"::STRING,
            t.$1:"b_sprog_cd_2"::STRING,
            t.$1:"b_sprog_cd_3"::STRING,
            t.$1:"b_sprog_cd_4"::STRING,
            t.$1:"b_app_pvrty_lvl_cd_1"::STRING,
            t.$1:"b_app_pvrty_lvl_cd_2"::STRING,
            t.$1:"b_app_pvrty_lvl_cd_3"::STRING,
            t.$1:"b_app_pvrty_lvl_cd_4"::STRING,
            t.$1:"b_specl_nd_cd"::STRING,
            t.$1:"p_npi_num"::STRING,
            t.$1:"p_locator_cd"::STRING,
            t.$1:"b_fund_split_cd"::STRING,
            t.$1:"b_reg_cd"::STRING,
            t.$1:"g_aud_add_user_id"::STRING,
            t.$1:"g_aud_add_ts"::TIMESTAMP,
            t.$1:"g_aud_user_id"::STRING,
            t.$1:"g_aud_ts"::TIMESTAMP,
            t.$1:"b_ca_exmt_cd"::STRING,
            t.$1:"r_payr_id"::INT,
            t.$1:"r_hlth_pln_id"::INT,
            t.$1:"b_enrl_fee_dt"::DATE,
            t.$1:"b_admin_cnty_cd"::STRING,
            t.$1:"b_inc_aft_tax_amt"::DECIMAL(9,2),
            t.$1:"b_gross_inc_aft_tax_amt"::DECIMAL(9,2),
            t.$1:"b_gross_inc_amt"::DECIMAL(9,2),
            t.$1:"b_tot_fml_cnt"::INT,
            t.$1:"b_fam_chld_cnt"::INT,
            t.$1:"b_fam_adult_cnt"::INT
        FROM @extstage_recipient_dagster/b_elig_spn_tb.parquet (FILE_FORMAT => parquet_format) t
        )
        FILE_FORMAT = parquet_format
        LOAD_MODE = FULL_INGEST;
        """,
        """
        COPY INTO RAW.B_ENRL_TB (
        ETL_DATA_SOURCE,
        ETL_BATCH_ID,
        DATE_INSERT_ROW,
        R_BNFT_PLN_ID,
        B_SYS_ID,
        B_ENRL_BEG_DT,
        B_ELIG_ID_NUM,
        B_ENRL_END_DT,
        B_ST_AUTH_CD,
        B_LV_ARRANGE_CD,
        B_PIED_CD,
        B_LME_SYS_ID,
        B_LME_NPI_ID,
        B_PCP_ID,
        B_BLNG_P_ID,
        B_SHR_ALCTN_CD,
        B_ADMIN_ENTY_ID,
        B_CA_EXMT_CD,
        B_THRSHLD_MET_DT,
        B_ECG_IND,
        B_ELIG_COV_CD,
        B_REC_STAT_CD,
        G_AUD_ADD_USER_ID,
        G_AUD_ADD_TS,
        G_AUD_USER_ID,
        G_AUD_TS,
        P_NPI_NUM,
        P_LOCATOR_CD,
        B_ADAP_GRP_ID
        )
        FROM (
        SELECT
            'RECIPIENT_DAGSTER' AS ETL_DATA_SOURCE,
            1 AS ETL_BATCH_ID,
            CURRENT_TIMESTAMP AS DATE_INSERT_ROW,
            t.$1:"r_bnft_pln_id"::INT,
            t.$1:"b_sys_id"::INT,
            t.$1:"b_enrl_beg_dt"::DATE,
            t.$1:"b_elig_id_num"::INT,
            t.$1:"b_enrl_end_dt"::DATE,
            t.$1:"b_st_auth_cd"::STRING,
            t.$1:"b_lv_arrange_cd"::STRING,
            t.$1:"b_pied_cd"::STRING,
            t.$1:"b_lme_sys_id"::STRING,
            t.$1:"b_lme_npi_id"::STRING,
            t.$1:"b_pcp_id"::STRING,
            t.$1:"b_blng_p_id"::STRING,
            t.$1:"b_shr_alctn_cd"::STRING,
            t.$1:"b_admin_enty_id"::STRING,
            t.$1:"b_ca_exmt_cd"::STRING,
            t.$1:"b_thrshld_met_dt"::DATE,
            t.$1:"b_ecg_ind"::STRING,
            t.$1:"b_elig_cov_cd"::STRING,
            t.$1:"b_rec_stat_cd"::STRING,
            t.$1:"g_aud_add_user_id"::STRING,
            t.$1:"g_aud_add_ts"::TIMESTAMP,
            t.$1:"g_aud_user_id"::STRING,
            t.$1:"g_aud_ts"::TIMESTAMP,
            t.$1:"p_npi_num"::STRING,
            t.$1:"p_locator_cd"::STRING,
            t.$1:"b_adap_grp_id"::STRING
        FROM @extstage_recipient_dagster/b_enrl_tb.parquet (FILE_FORMAT => parquet_format) t
        )
        FILE_FORMAT = parquet_format
        LOAD_MODE = FULL_INGEST;
        """,
        """
        COPY INTO RAW.B_LANG_TB (
        ETL_DATA_SOURCE,
        ETL_BATCH_ID,
        DATE_INSERT_ROW,
        B_SYS_ID,
        B_LANG_CD,
        B_PRIM_IND,
        B_REC_STAT_CD,
        G_AUD_ADD_USER_ID,
        G_AUD_ADD_TS,
        G_AUD_USER_ID,
        G_AUD_TS
        )
        FROM (
        SELECT
            'RECIPIENT_DAGSTER' AS ETL_DATA_SOURCE,
            1 AS ETL_BATCH_ID,
            CURRENT_TIMESTAMP AS DATE_INSERT_ROW,
            t.$1:"b_sys_id"::INT,
            t.$1:"b_lang_cd"::STRING,
            t.$1:"b_prim_ind"::STRING,
            t.$1:"b_rec_stat_cd"::STRING,
            t.$1:"g_aud_add_user_id"::STRING,
            t.$1:"g_aud_add_ts"::TIMESTAMP,
            t.$1:"g_aud_user_id"::STRING,
            t.$1:"g_aud_ts"::TIMESTAMP
        FROM @extstage_recipient_dagster/b_lang_tb.parquet (FILE_FORMAT => parquet_format) t
        )
        FILE_FORMAT = parquet_format
        LOAD_MODE = FULL_INGEST;
        """,
        """
        
        COPY INTO RAW.B_RACE_TB (
        ETL_DATA_SOURCE,
        ETL_BATCH_ID,
        DATE_INSERT_ROW,
        B_SYS_ID,
        B_RACE_CD,
        B_REC_STAT_CD,
        G_AUD_ADD_USER_ID,
        G_AUD_ADD_TS,
        G_AUD_USER_ID,
        G_AUD_TS
        )
        FROM (
        SELECT
            'RECIPIENT_DAGSTER' AS ETL_DATA_SOURCE,
            1 AS ETL_BATCH_ID,
            CURRENT_TIMESTAMP AS DATE_INSERT_ROW,
            t.$1:"b_sys_id"::INT,
            t.$1:"b_race_cd"::STRING,
            t.$1:"b_rec_stat_cd"::STRING,
            t.$1:"g_aud_add_user_id"::STRING,
            t.$1:"g_aud_add_ts"::TIMESTAMP,
            t.$1:"g_aud_user_id"::STRING,
            t.$1:"g_aud_ts"::TIMESTAMP
        FROM @extstage_recipient_dagster/b_race_tb.parquet (FILE_FORMAT => parquet_format) t
        )
        FILE_FORMAT = parquet_format
        LOAD_MODE = FULL_INGEST;
        """,
        """
        COPY INTO RAW.B_XREF_TB (
        ETL_DATA_SOURCE,
        ETL_BATCH_ID,
        DATE_INSERT_ROW,
        B_SYS_ID,
        B_ALT_ID,
        B_ALT_ID_TY_CD,
        B_ALT_ID_EFF_DT,
        B_ALT_ID_END_DT,
        B_BASE_IND,
        B_REC_STAT_CD,
        G_AUD_ADD_USER_ID,
        G_AUD_ADD_TS,
        G_AUD_USER_ID,
        G_AUD_TS,
        G_H_SYS_ID_PTK
        )
        FROM (
        SELECT
            'RECIPIENT_DAGSTER' AS ETL_DATA_SOURCE,
            1 AS ETL_BATCH_ID,
            CURRENT_TIMESTAMP AS DATE_INSERT_ROW,
            t.$1:"b_sys_id"::INT,
            t.$1:"b_alt_id"::STRING,
            t.$1:"b_alt_id_ty_cd"::STRING,
            t.$1:"b_alt_id_eff_dt"::DATE,
            t.$1:"b_alt_id_end_dt"::DATE,
            t.$1:"b_base_ind"::STRING,
            t.$1:"b_rec_stat_cd"::STRING,
            t.$1:"g_aud_add_user_id"::STRING,
            t.$1:"g_aud_add_ts"::TIMESTAMP,
            t.$1:"g_aud_user_id"::STRING,
            t.$1:"g_aud_ts"::TIMESTAMP,
            t.$1:"g_h_sys_id_ptk"::NUMBER
        FROM @extstage_recipient_dagster/b_xref_tb.parquet (FILE_FORMAT => parquet_format) t
        )
        FILE_FORMAT = parquet_format
        LOAD_MODE = FULL_INGEST;
        """
    ]

    for sql in sql_statements:
        try:
            session.sql(sql).collect()
            print(" Executed COPY INTO successfully.")
        except Exception as e:
            print(f" Failed to execute COPY INTO: {e}")

    session.close()
    return Output(
        value=None,
        metadata={
            "asset_type": "Snowflake Table",
            "tables_loaded": ["RAW.B_ADDR_TB", "RAW.B_AUTH_REP_TB"]
            
        }
    )
