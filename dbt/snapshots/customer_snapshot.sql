{% snapshot customer_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['address', 'phone_number', 'account_balance'],
      invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('stg_customer') }}

{% endsnapshot %}