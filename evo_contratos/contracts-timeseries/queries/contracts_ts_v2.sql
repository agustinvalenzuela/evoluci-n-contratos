WITH RECURSIVE filas AS (
SELECT *,
	ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY fecha_inicio DESC, fecha_fin DESC, id DESC) AS rn
FROM assetplan_rentas.contrato_arriendos
),
contratos AS(
SELECT
  	id as CID2,
  	contrato,
	property_id,
	renter_id,
	CONCAT(property_id, '-', renter_id) AS contract_id,
	fecha_inicio,
	CASE
		WHEN fecha_retiro_efectiva = '0000-00-00' OR fecha_retiro_efectiva = '0000-00-01'
			THEN fecha_fin
		WHEN fecha_retiro_efectiva IS NULL AND fecha_fin < CURDATE() AND rn = 1
			THEN DATE_ADD(CURDATE(), INTERVAL 1 MONTH)
		ELSE fecha_retiro_efectiva
	END AS fecha_retiro_efectiva,
	fecha_fin,
	monto_arriendo
FROM filas
WHERE property_id IS NOT NULL
	AND renter_id IS NOT NULL
	AND deleted_at IS NULL
	AND pais_id   = 1
	ORDER BY property_id, fecha_inicio
),
fechas AS (
SELECT
	CID2,
	contrato,
	contract_id,
	property_id,
	renter_id,
	monto_arriendo,
	CASE
		WHEN YEAR(fecha_inicio) < 2017 AND YEAR(fecha_retiro_efectiva) >= 2017
			THEN DATE('2017-01-01')
		ELSE fecha_inicio
	END AS fecha_inicio,
	fecha_fin,
	fecha_retiro_efectiva
FROM contratos
WHERE fecha_inicio     != '0000-00-00'
	AND fecha_inicio   IS NOT NULL
	AND monto_arriendo >= 150000
ORDER BY property_id
),
termino_contrato AS (
SELECT
	CID2,
	contrato,
	property_id,
	renter_id,
	contract_id,
	monto_arriendo,
	fecha_inicio,
	fecha_retiro_efectiva,
	fecha_fin,
	CASE
		-- Caso 1: renovación normal (mismo contract_id) con fecha_inicio posterior
		--         → cierro al inicio del siguiente
		WHEN fecha_retiro_efectiva IS NULL
		 AND LEAD(contract_id)  OVER w = contract_id
		 AND LEAD(fecha_inicio) OVER w > fecha_inicio
			THEN LEAD(fecha_inicio) OVER w

		-- Caso 2: renovación con MISMA fecha_inicio (error humano)
		--         → no puedo usar LEAD(fecha_inicio), uso fecha_fin del contrato actual
		WHEN fecha_retiro_efectiva IS NULL
		 AND LEAD(contract_id)  OVER w = contract_id
		 AND LEAD(fecha_inicio) OVER w = fecha_inicio
			THEN fecha_fin

		-- Caso 3: siguiente contrato es de OTRO renter en la misma propiedad
		--         → cierro al inicio del siguiente para evitar solapes
		WHEN fecha_retiro_efectiva IS NULL
		 AND LEAD(property_id)  OVER w = property_id
		 AND LEAD(fecha_inicio) OVER w > fecha_inicio
			THEN LEAD(fecha_inicio) OVER w

		-- Caso 4: último contrato de la propiedad sin retiro efectivo
		WHEN fecha_retiro_efectiva IS NULL
			THEN fecha_fin

		ELSE fecha_retiro_efectiva
	END AS fecha_termino,
	ROW_NUMBER() OVER w AS rn
FROM fechas
WHERE YEAR(fecha_inicio) > 2016
WINDOW w AS (PARTITION BY property_id ORDER BY fecha_inicio, fecha_fin, CID2)
),
ajuste_inicio AS (
-- Si el contrato anterior termina DESPUÉS de la fecha_inicio actual (solape),
-- empujo la fecha_inicio actual al cierre del anterior para eliminar el cruce.
SELECT
	CID2,
	contrato,
	property_id,
	renter_id,
	contract_id,
	monto_arriendo,
	CASE
		WHEN LAG(fecha_termino) OVER w >= fecha_inicio
			THEN LAG(fecha_termino) OVER w
		ELSE fecha_inicio
	END AS fecha_inicio,
	fecha_retiro_efectiva,
	fecha_fin,
	fecha_termino
FROM termino_contrato
WINDOW w AS (PARTITION BY property_id ORDER BY fecha_inicio, fecha_fin, CID2)
),
termino_anterior AS (
SELECT
	CID2,
	contrato,
	property_id,
	renter_id,
	contract_id,
	monto_arriendo,
	fecha_inicio,
	fecha_retiro_efectiva,
	fecha_fin,
	fecha_termino,
	COALESCE(
		LAG(fecha_termino) OVER (PARTITION BY property_id ORDER BY fecha_inicio, fecha_fin, CID2),
		DATE('2017-01-01')
	) AS fin_contrato_anterior
FROM ajuste_inicio
ORDER BY property_id, fecha_inicio
),
date_diffs AS (
SELECT
	*,
	ROUND(DATEDIFF(fecha_inicio,fin_contrato_anterior)/30.44, 0) AS diff_cont,
	ROUND(DATEDIFF(fecha_termino,fecha_inicio)/30.44, 0) AS duracion,
	1 AS period_id
FROM termino_anterior
),
final AS (
SELECT
    CID2, -- 1
    contrato,
    renter_id, -- 2
    property_id, -- 3
    period_id, -- 4
    contract_id, -- 5
    monto_arriendo, -- 6
    fin_contrato_anterior, -- 7
    fecha_inicio, -- 8
    fecha_retiro_efectiva, -- 9
    fecha_fin, -- 10
    fecha_termino -- 11
FROM date_diffs
),
gaps AS (
    SELECT
        NULL AS CID2, -- 1
        NULL AS contrato,
        NULL AS renter_id, -- 2
        property_id, -- 3
        0 AS period_id, -- 4
        CONCAT(
            property_id, '-NOCONTRACT-',
            ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY fecha_inicio, fecha_fin, CID2)
           ) AS contract_id, -- 5
        NULL AS monto_arriendo, -- 6
        NULL AS fin_contrato_anterior, -- 7
        fin_contrato_anterior AS fecha_inicio, -- 8
        NULL AS fecha_retiro_efectiva, -- 9
        NULL AS fecha_fin, -- 10
        fecha_inicio AS fecha_termino -- 11
    FROM date_diffs
    WHERE diff_cont > 0 -- SOLO GENERAR GAP CUANDO EXISTE
),
juntar AS (
SELECT * FROM final
UNION ALL
SELECT * FROM gaps
ORDER BY property_id, fecha_inicio
)SELECT
*,
ROUND(DATEDIFF(fecha_retiro_efectiva, fecha_inicio) / 30.44, 0) AS duracion_periodo
FROM juntar