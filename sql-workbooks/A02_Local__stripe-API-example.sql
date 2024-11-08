-- ----------------------------------------------------------------------
-- Write your question with "--" prefix (SQL comment), highlight it (as with SQL clause)
-- ----------------------------------------------------------------------
-- Show total cost for current year
-- DROP SECRET http;
CREATE SECRET http (
  type http,
  extra_http_headers map {
    'Authorization': 'Bearer sk_test_VePHdqKTYQjKNInc7u56JBrQ'
  },
  scope 'https://api.stripe.com/'
);

WITH nested AS (
   SELECT unnest (data) AS payment_intents
     FROM read_json ('https://api.stripe.com/v1/payment_intents')
) SELECT payment_intents.* FROM nested;

WITH nested AS (
   SELECT unnest (data) AS customers
     FROM read_json ('https://api.stripe.com/v1/customers')
) SELECT customers.* FROM nested;
