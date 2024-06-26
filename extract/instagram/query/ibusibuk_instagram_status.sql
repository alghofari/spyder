SELECT
  username
FROM
  `sirclo-prod.bronze_instagram.ibusibuk_instagram_status`
WHERE
  is_profile_exist IS FALSE
  AND RAND() < 10/1000
LIMIT
  10