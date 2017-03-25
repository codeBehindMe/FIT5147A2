/*******************************************************************
* Author :: Aaron Tillekeratne                                     *
* Student Number :: 27345483                                       *
* DB :: Simply Energy Enterprise Data Warehouse                    *
* Copyright :: Simply Energy Private Data Copyright (See Author)   *
*                                                                  *
*******************************************************************/

--
-- SELECT
--   NMI.Nmi,
--   CONVERT(DATE, DTE.full_date, 103) AS DTE,
--   IMS.ReadValues                    AS READ_VALUE,
--   IMS.intervalReadDayId
-- FROM CDM.cdm.v_interval_meter_detail_interval_meter_sum;mary AS IMS
--   INNER JOIN CDM.cdm.dim_date AS DTE ON ims.sk_dim_date_id = DTE.sk_dim_date_id
--   INNER JOIN CDM.cdm.dim_nmi AS NMI ON ims.sk_dim_nmi_id = NMI.sk_dim_nmi_id
-- WHERE CONVERT(DATE, DTE.full_date, 103) >= CONVERT(DATE, '01/01/2015', 103) AND
--       CONVERT(DATE, DTE.full_date, 103) < CONVERT(DATE, '01/01/2016', 103)
--       AND nmi.State = 'VIC';
--
-- -- Explore in Batches
-- CREATE TABLE ADM_AT.NMI_VIC_2015 AS (
--
-- SELECT * FROM CDM.cdm.
--
-- )

-- Batch nmi dimension
SELECT
  sk_dim_nmi_id,
  Nmi,
  ParticipantCode,
  NmiStatusCode,
  NmiClassCode,
  PostCode,
  DpId,
  State
INTO
  SANDPIT_REPORTING.ADM_AT.NMI_VIC_2015
FROM CDM.cdm.dim_nmi AS nmi
WHERE nmi.State = 'VIC';

CREATE INDEX IX_AT_NMI_VIC ON ADM_AT.NMI_VIC_2015(sk_dim_nmi_id,Nmi,PostCode);



-- batch date dimension
SELECT sk_dim_date_id,
full_date
INTO SANDPIT_REPORTING.ADM_AT.DATE_VIC_2015
FROM CDM.cdm.dim_date AS DTE
WHERE CONVERT(DATE,DTE.full_date,103) >= CONVERT(DATE,'01/01/2016',103) AND CONVERT(DATE,DTE.full_date,103) < CONVERT(DATE,'01/01/2017',103);

CREATE INDEX IX_AT_DTE_VIC ON ADM_AT.DATE_VIC_2015(sk_dim_date_id);


-- Batch the summary

SELECT a.sk_fct_interval_meter_summary_id,b.*,c.*,a.ReadValueSum
INTO SANDPIT_REPORTING.ADM_AT.SUMM_VIC_2015
FROM CDM.cdm.fct_interval_meter_summary AS a 
INNER JOIN SANDPIT_REPORTING.ADM_AT.NMI_VIC_2015 AS b ON a.sk_dim_nmi_id = b.sk_dim_nmi_id
INNER JOIN SANDPIT_REPORTING.ADM_AT.DATE_VIC_2015 AS c ON a.sk_dim_date_id = c.sk_dim_date_id;

CREATE INDEX IX_AT_MSID_VIC ON ADM_AT.SUMM_VIC_2015(sk_fct_interval_meter_summary_id);


-- We need the NMIs only of which we have a full year of data on.
--SELECT NMI,count(full_date) FROM ADM_AT.SUMM_VIC_2015
--GROUP BY Nmi