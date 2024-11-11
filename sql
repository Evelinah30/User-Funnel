with
users_from_braze as (
    select   id as braze_external_id,
             external_id,
             email_address,
             case when custom_utm_region = ' ' or custom_utm_region is null or NULLIF(TRIM(custom_utm_region), '') is null then 'AU'
                  when custom_region = ' ' or custom_region is null or NULLIF(TRIM(custom_region), '') is null then 'AU'
                  else coalesce(upper(custom_utm_region), upper(custom_region), 'AU')
             end as user_region
    from user_data
   where email is not null
), 

-- ACCOUNT REGISTERED--
registered_account_at as (
  select user_id as braze_external_id,
         min(cast(original_timestamp as timestamp_ntz)) as registered_account_at
    from api_signin_data
   group by 1
),

-- APPLICATION SUBMITTED
submitted_at as (
  select user_id as braze_external_id,
         id as event_id,
         min(cast(original_timestamp as timestamp_ntz)) as submitted_at 
    from api_card_data
   group by 1, 2
), 

-- APPROVED CARDHOLDERS
onboarding_passed_at as (
  select user_id as braze_external_id,
         id as event_id,
         min(cast(original_timestamp as timestamp_ntz)) as onboarding_passed_at
    from api_communications_data
   where card_event_type = 'ONBOARDING_PASSED'
   group by 1, 2
),

-- CARD LINKED
has_card_linked_succesfully as (
  select braze_external_id,
         event_id,
         first_accepted_mandate_at
  from (
        select user_id as braze_external_id,
             first_value(id) over (partition by user_id order by original_timestamp asc) as event_id,
             first_value(original_timestamp) over (partition by user_id order by original_timestamp asc) as first_accepted_mandate_at
        from api_communications_data
        where card_event_type = 'MANDATE'
       )
),

-- DIGITAL CARDS: CARDS ADDED TO DIGITAL WALLET
has_active_digital_card as (
  select user_id as braze_external_id,
         min(cast(original_timestamp as timestamp_ntz)) as active_digital_card_at 
    from api_communications_data
   where card_event_type = 'CARD_ADDED_TO_WALLET'
   group by 1
),

-- TRANSACTIONS
total_transactions as (
  select user_id as braze_external_id,
         count(distinct transaction_event_id) as total_transactions,
         min(cast(original_timestamp as timestamp_ntz)) as first_transaction_at,
         max(cast(original_timestamp as timestamp_ntz)) as last_transaction_at,
         round(sum(abs(transaction_amount)), 2) as total_amount_spent
    from api_transactions_data
   where upper(transaction_event_outcome) = 'ACCEPTED'
     and upper(transaction_currency) = 'AUD'
   group by 1
),

-- CARD PHYSICAL: 
active_physical_card_at as (
  select user_id as braze_external_id,
         min(cast(original_timestamp as timestamp_ntz)) as active_physical_card_at 
    from api_wallet_data
   group by 1
)

select 
    count(distinct external_id) as total_users,
    count(case when has_registered_account then 1 end) as total_registered_accounts,
    count(case when has_submitted_at then 1 end) as total_applications_submitted,
    count(case when has_approved_cardholders then 1 end) as total_approved_cardholders,
    count(case when has_card_linked_succesfully then 1 end) as total_with_card_linked_succesfully,
    count(case when has_active_digital_card then 1 end) as total_with_active_digital_card,
    count(case when total_transactions > 0 then 1 end) as total_with_transactions,
    count(case when has_active_physical_card then 1 end) as total_with_active_physical_card
from (
    select a.*,
           b.registered_account_at,
           case when b.registered_account_at is not null then true else false end as has_registered_account,
           c.submitted_at,
           case when c.submitted_at is not null then true else false end as has_submitted_at,
           d.onboarding_passed_at,
           case when d.onboarding_passed_at is not null then true else false end as has_approved_cardholders,
           e.first_accepted_mandate_at,
           case when e.first_accepted_mandate_at is not null then true else false end as has_card_linked_succesfully,
           f.active_digital_card_at,
           case when f.active_digital_card_at is not null then true else false end as has_active_digital_card,
           g.total_transactions,
           h.active_physical_card_at,
           case when h.active_physical_card_at is not null then true else false end as has_active_physical_card
    from user_data a
    left join registered_account_at b on a.external_id = b.braze_external_id
    left join submitted_at c on a.external_id = c.braze_external_id
    left join onboarding_passed_at d on a.external_id = d.braze_external_id
    left join has_card_linked_succesfully e on a.external_id = e.braze_external_id
    left join has_active_digital_card f on a.external_id = f.braze_external_id
    left join total_transactions g on a.external_id = g.braze_external_id
    left join active_physical_card_at h on a.external_id = h.braze_external_id
    );
