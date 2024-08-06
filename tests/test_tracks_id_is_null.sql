SELECT *
FROM {{ ref('tracks') }}
WHERE id IS NULL