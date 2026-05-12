WITH RECURSIVE fechas AS (
SELECT *,
CASE
	WHEN CAST(fecha_retiro_efectiva AS CHAR) = '0000-00-01'
		THEN fecha_fin
	WHEN fecha_retiro_efectiva IS NULL
		THEN fecha_fin
	ELSE fecha_retiro_efectiva
END
FROM (
    SELECT *,
        CAST(fecha_retiro_efectiva AS CHAR) AS fecha_retiro_raw
    FROM assetplan_rentas.contrato_arriendos
) raw
),
contratos AS(
SELECT
  	id as CID2,
	property_id,
	renter_id,
	CONCAT(property_id, '-', renter_id) AS contract_id,
	DATE_FORMAT(fecha_inicio, '%Y-%m-01') AS fecha_inicio,
	DATE_FORMAT(fecha_retiro_efectiva, '%Y-%m-01') AS fecha_retiro_efectiva,
	DATE_FORMAT(fecha_fin, '%Y-%m-01') AS fecha_fin,
	monto_arriendo,
	COALESCE(
    LAG(DATE_FORMAT(fecha_retiro_efectiva, '%Y-%m-01')) OVER (PARTITION BY property_id ORDER BY fecha_inicio),
    DATE('2017-01-01')) AS fin_contrato_anterior,
  	tipo_renovacion
FROM fechas
WHERE YEAR(fecha_inicio)        > 2016
	AND monto_arriendo          IS NOT NULL
	AND fecha_inicio            IS NOT NULL
	AND fecha_retiro_efectiva	IS NOT NULL
	AND property_id             IS NOT NULL
	AND renter_id               IS NOT NULL
	AND pais_id                 = 1
	AND monto_arriendo          >= 150000
	AND fecha_retiro_raw != '0000-00-01'
    AND fecha_retiro_raw != '0000-00-00'
	ORDER BY property_id, fecha_inicio
),
tabla AS (
SELECT *,
	ROUND(DATEDIFF(fecha_inicio, fin_contrato_anterior)/30.44,0) AS diff_entre_contratos
FROM contratos
),
final AS (
SELECT
    CID2, -- 1
    renter_id, -- 2
    property_id, -- 3
    1 AS period_id, -- 4 Identificar periodos de contrato y no contrato
    contract_id, -- 5
    monto_arriendo, -- 6
    fin_contrato_anterior, -- 7
    fecha_inicio, -- 8
    fecha_retiro_efectiva, -- 9
    fecha_fin -- 10
FROM tabla
),
gaps AS (
    SELECT
        NULL AS CID2, -- 1
        NULL AS renter_id, -- 2
        property_id, -- 3
        0 AS period_id, -- 4 Identificar periodos de contrato y no contrato
        CONCAT(
            property_id, '-NOCONTRACT-',
            ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY fecha_inicio)
        ) AS contract_id, -- 5
        NULL AS monto_arriendo, -- 6
        NULL AS fin_contrato_anterior, -- 7
        fin_contrato_anterior AS fecha_inicio, -- 8
        NULL AS fecha_retiro_efectiva, -- 9
        NULL AS fecha_fin -- 10
    FROM final
    WHERE ROUND(DATEDIFF(fecha_inicio, fin_contrato_anterior) / 30.44, 0) > 0 -- SOLO GENERAR GAP CUANDO EXISTE
),
juntar AS (
SELECT * FROM final
UNION ALL
SELECT * FROM gaps
ORDER BY property_id, fecha_inicio
),
completo AS (
SELECT 
	CID2 AS contract_id,
	renter_id,
	property_id,
	period_id AS en_contrato,
	contract_id AS property_renter_id,
	monto_arriendo,
	fecha_inicio,
	CASE
		WHEN fecha_retiro_efectiva IS NULL 
		AND LEAD(property_id) OVER (PARTITION BY property_id ORDER BY fecha_inicio) = property_id
			THEN LEAD(fecha_inicio) OVER (PARTITION BY property_id ORDER BY fecha_inicio)
		ELSE fecha_retiro_efectiva
	END AS fecha_retiro_efectiva
FROM juntar
),
fecha_lag AS (
SELECT*,
	COALESCE(
    LAG(DATE_FORMAT(fecha_retiro_efectiva, '%Y-%m-01')) OVER (PARTITION BY property_id ORDER BY fecha_inicio),
    DATE('2017-01-01')) AS fin_contrato_anterior
FROM completo
),
duracion AS (
 SELECT*,
	ROUND(DATEDIFF(fecha_retiro_efectiva, fecha_inicio) / 30.44, 0) AS duracion_periodo
 FROM fecha_lag
 ORDER BY property_id, fecha_inicio
 )
 SELECT*FROM duracion