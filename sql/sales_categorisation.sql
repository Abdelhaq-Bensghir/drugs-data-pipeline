SELECT
    t.client_id,
    SUM(CASE
        WHEN pn.product_type = 'MEUBLE' THEN t.prod_price * t.prod_qty
        ELSE 0
    END) AS ventes_meuble,
    SUM(CASE
        WHEN pn.product_type = 'DECO' THEN t.prod_price * t.prod_qty
        ELSE 0
    END) AS ventes_deco
FROM
    transactions AS t
INNER JOIN
    product_nomenclature AS pn ON t.prod_id = pn.product_id
WHERE
    t.date >= '2019-01-01' AND t.date <= '2019-12-31'
GROUP BY
    t.client_id
ORDER BY
    t.client_id;
