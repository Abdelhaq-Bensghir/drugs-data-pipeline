SELECT
    date,
    SUM(prod_price * prod_qty) AS ventes
FROM
    transactions
WHERE
    date >= '2019-01-01' AND date <= '2019-12-31'
GROUP BY
    date
ORDER BY
    date;
