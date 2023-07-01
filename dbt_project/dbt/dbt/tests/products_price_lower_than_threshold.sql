SELECT
*
FROM
{{ ref('bronze_products') }}
WHERE price < 10
