USE CDM;
SELECT TOP 5 summ.[sk_fct_interval_meter_summary_id],summ.[ReadValueSum] , dte.date, nmi.Nmi,nmi.State,nmi.PostCode, nmi.participantcode, nmi.nmistatuscode,nmi.nmiclasscode
 FROM cdm.fct_interval_meter_summary summ
INNER JOIN cdm.dim_date DTE ON summ.sk_dim_date_id = dte.sk_dim_date_id
INNER JOIN cdm.dim_nmi nmi on summ.sk_dim_nmi_id = nmi.sk_dim_nmi_id
INNER JOIN cdm.dim_uom uom ON summ.sk_dim_uom_id = uom.sk_dim_uom_id
WHERE 
CONVERT(DATE,DTE.full_date,103) >= CONVERT(DATE,'01/01/2016',103) AND CONVERT(DATE,DTE.full_date,103) < CONVERT(DATE,'01/01/2017',103)
AND nmi.State = 'VIC'
AND uom.Uom = 'KWH'




WITH cteReads AS(
SELECT summ.[sk_fct_interval_meter_summary_id],summ.[ReadValueSum] , dte.date, nmi.Nmi,nmi.State,nmi.PostCode, nmi.participantcode, nmi.nmistatuscode,nmi.nmiclasscode
 FROM cdm.fct_interval_meter_summary summ
INNER JOIN cdm.dim_date DTE ON summ.sk_dim_date_id = dte.sk_dim_date_id
INNER JOIN cdm.dim_nmi nmi on summ.sk_dim_nmi_id = nmi.sk_dim_nmi_id
INNER JOIN cdm.dim_uom uom ON summ.sk_dim_uom_id = uom.sk_dim_uom_id
WHERE 
CONVERT(DATE,DTE.full_date,103) >= CONVERT(DATE,'01/01/2016',103) AND CONVERT(DATE,DTE.full_date,103) < CONVERT(DATE,'01/01/2017',103)
AND nmi.State = 'VIC'
AND uom.Uom = 'KWH' 
)
SELECT nmi, count(*), min(date),max(date), count(distinct date) FROM cteReads
group by nmi 
HAVING count(distinct date) >= 300
AND min(date) = '2016-01-01 00:00:00.000' AND MAX(date) = '2016-12-31 00:00:00.000'
order by 5 desc;







WITH cteReads AS(
SELECT summ.[sk_fct_interval_meter_summary_id],summ.[ReadValueSum] , dte.date, nmi.Nmi,nmi.State,nmi.PostCode, nmi.participantcode, nmi.nmistatuscode,nmi.nmiclasscode
 FROM cdm.fct_interval_meter_summary summ
INNER JOIN cdm.dim_date DTE ON summ.sk_dim_date_id = dte.sk_dim_date_id
INNER JOIN cdm.dim_nmi nmi on summ.sk_dim_nmi_id = nmi.sk_dim_nmi_id
INNER JOIN cdm.dim_uom uom ON summ.sk_dim_uom_id = uom.sk_dim_uom_id
WHERE 
CONVERT(DATE,DTE.full_date,103) >= CONVERT(DATE,'01/01/2016',103) AND CONVERT(DATE,DTE.full_date,103) < CONVERT(DATE,'01/01/2017',103)
AND nmi.State = 'VIC'
AND uom.Uom = 'KWH' 
)
SELECT nmi,participantcode--, count(*), min(date),max(date), count(distinct date) 
into #NMI_LIST
FROM cteReads
group by nmi , participantcode
HAVING count(distinct date) >= 300
AND min(date) = '2016-01-01 00:00:00.000' AND MAX(date) = '2016-12-31 00:00:00.000';



SELECT summ.[sk_fct_interval_meter_summary_id],summ.[ReadValueSum] , dte.date, nmi.Nmi,nmi.State,nmi.PostCode, nmi.participantcode, nmi.nmistatuscode,nmi.nmiclasscode
 INTO SANDPIT_REPORTING.ADM_AT.NMI_SUMMARY_READS
 FROM cdm.fct_interval_meter_summary summ
INNER JOIN cdm.dim_date DTE ON summ.sk_dim_date_id = dte.sk_dim_date_id
INNER JOIN cdm.dim_nmi nmi on summ.sk_dim_nmi_id = nmi.sk_dim_nmi_id
INNER JOIN cdm.dim_uom uom ON summ.sk_dim_uom_id = uom.sk_dim_uom_id
INNER JOIN #NMI_LIST nmil ON nmil.Nmi = nmi.Nmi AND nmil.ParticipantCode = nmi.ParticipantCode
WHERE 
CONVERT(DATE,DTE.full_date,103) >= CONVERT(DATE,'01/01/2016',103) AND CONVERT(DATE,DTE.full_date,103) < CONVERT(DATE,'01/01/2017',103)
AND nmi.State = 'VIC'
AND uom.Uom = 'KWH';


CREATE CLUSTERED INDEX IX_NMI_SUMMARY_READS ON SANDPIT_REPORTING.ADM_AT.NMI_SUMMARY_READS([sk_fct_interval_meter_summary_id]);

TRUNCATE TABLE SANDPIT_REPORTING.ADM_RT.INTERVAL_METER_READS_DET;
DECLARE @lastsk INT;
SET @lastsk = (SELECT MIN([sk_fct_interval_meter_summary_id]) - 1 FROM SANDPIT_REPORTING.ADM_AT.NMI_SUMMARY_READS)

WHILE EXISTS ( SELECT TOP 1 1 FROM SANDPIT_REPORTING.ADM_AT.NMI_SUMMARY_READS WHERE [sk_fct_interval_meter_summary_id] > @lastsk)
BEGIN
	BEGIN TRY
		DROP TABLE #loop
	END TRY
	BEGIN CATCH
	END CATCH

	SELECT TOP 100000 * INTO #LOOP FROM SANDPIT_REPORTING.ADM_AT.NMI_SUMMARY_READS WHERE [sk_fct_interval_meter_summary_id] > @lastsk
	ORDER BY [sk_fct_interval_meter_summary_id]

	INSERT INTO SANDPIT_REPORTING.ADM_RT.INTERVAL_METER_READS_DET
	SELECT a.*,b.standard_period_id,b.ReadValues FROM #LOOP a INNER JOIN CDM.cdm.fct_interval_meter_detail b on a.[sk_fct_interval_meter_summary_id] = b.[sk_fct_interval_meter_summary_id]


	SET @lastsk = (SELECT MAX([sk_fct_interval_meter_summary_id]) FROM #LOOP)


END


CREATE PROCEDURE ADM_AT.SP_USAGE_2016_LOAD AS 

TRUNCATE TABLE SANDPIT_REPORTING.ADM_RT.INTERVAL_METER_READS_DET;
DECLARE @lastsk INT;
SET @lastsk = (SELECT MIN([sk_fct_interval_meter_summary_id]) - 1 FROM SANDPIT_REPORTING.ADM_AT.NMI_SUMMARY_READS)

WHILE EXISTS ( SELECT TOP 1 1 FROM SANDPIT_REPORTING.ADM_AT.NMI_SUMMARY_READS WHERE [sk_fct_interval_meter_summary_id] > @lastsk)
BEGIN
	BEGIN TRY
		DROP TABLE #loop
	END TRY
	BEGIN CATCH
	END CATCH

	SELECT TOP 100000 * INTO #LOOP FROM SANDPIT_REPORTING.ADM_AT.NMI_SUMMARY_READS WHERE [sk_fct_interval_meter_summary_id] > @lastsk
	ORDER BY [sk_fct_interval_meter_summary_id]

	INSERT INTO SANDPIT_REPORTING.ADM_RT.INTERVAL_METER_READS_DET
	SELECT a.*,b.standard_period_id,b.ReadValues FROM #LOOP a INNER JOIN CDM.cdm.fct_interval_meter_detail b on a.[sk_fct_interval_meter_summary_id] = b.[sk_fct_interval_meter_summary_id]


	SET @lastsk = (SELECT MAX([sk_fct_interval_meter_summary_id]) FROM #LOOP)

	CREATE INDEX IX_INTERVAL_METER_READS_DET ON SANDPIT_REPORTING.ADM_RT.INTERVAL_METER_READS_DET ([sk_fct_interval_meter_summary_id],Nmi,date,postcode)

END

