  -- A/B test start date (defaults to yesterday)
DECLARE abteststartdate DATE DEFAULT current_date-1;
  -- Date range for averaging metrics (defaults to last 28 days)                                                                                                                                         
  DECLARE daterange_start DATE DEFAULT abteststartdate-{lookback_days};                                                                                                                                                       
DECLARE daterange_end DATE DEFAULT abteststartdate;                                                                                                                                                     
  -- Base64-encoded JSON defining bucket weight ranges for each variant group (50/50 split)                                                                                                              
DECLARE abTestWeightedBucketsBase64 STRING DEFAULT TO_BASE64(CAST('[[{{"min":0,"max":49}}],[{{"min":50,"max":99}}]]' AS BYTES));
                                                                        
  -- Holds the array of random salts used to simulate multiple randomization seeds                                                                                                                       
  DECLARE seeds ARRAY <STRING>;                                                                                                                                                                          
                                                                                                                                                                                                         
  -- Assigns a user to a bucket (variant group) based on FNV-1a hash of salt+userId,                                                                                                                     
  -- mapped against weighted min/max ranges decoded from the base64 config                                                                                                                               
  CREATE TEMPORARY FUNCTION bucket(salt STRING, userId STRING, weightedBucketsJson STRING) RETURNS INT64                                                                                                 
  LANGUAGE js AS """
      // FNV 1a / 32bit                                                                                                                                                                                  
      function fnv32a(str) {{                                                                                                                                                                             
          const FNV1_32A_INIT = 0x811c9dc5;                                                                                                                                                              
          let hval = FNV1_32A_INIT;                                                                                                                                                                      
          for(let i = 0; i < str.length; ++i) {{                                                                                                                                                          
              hval ^= str.charCodeAt(i);                                                                                                                                                                 
              hval += (hval << 1) + (hval << 4) + (hval << 7) + (hval << 8) + (hval << 24);                                                                                                              
          }}                                                                                                                                                                                              
          return hval >>> 0;
      }};                                                                                                                                                                                                 
                                                                                                                                                                                                         
      // Bucket calculator                                                                                                                                                                               
      function calculateBucketMembership(salt, deviceId, weighedBuckets) {{                                                                                                                               
          let totalSize = 0;                                                                                                                                                                             
          for(let i = 0; i < weighedBuckets.length; i += 1) {{                                                                                                                                            
              for(let j = 0; j < weighedBuckets[i].length; j += 1) {{                                                                                                                                     
                  totalSize += weighedBuckets[i][j].max - weighedBuckets[i][j].min + 1;                                                                                                                  
              }}   
          }}                                                                                                                                                                                              
          const h = fnv32a(salt + deviceId) % totalSize;
          for(let i = 0; i < weighedBuckets.length; i += 1) {{
              for(let j = 0; j < weighedBuckets[i].length; j += 1) {{                                                                                                                                     
                  if(h >= weighedBuckets[i][j].min && h <= weighedBuckets[i][j].max) {{                                                                                                                   
                      return i;                                                                                                                                                                          
                  }}                                                                                                                                                                                      
              }}                                                                                                                                                                                          
          }}                                                                                                                                                                                              
          return weighedBuckets.length - 1;
      }}
      return calculateBucketMembership(salt, userId, JSON.parse(weightedBucketsJson));
  """;                                                                                                                                                                                                   
                                                                                                                                                                                                         
  -- Generates 30 random hex seeds used to simulate AA test randomization across multiple salts                                                                                                          
  CREATE TEMPORARY FUNCTION generateSeeds() RETURNS ARRAY<STRING>                                                                                                                                        
  LANGUAGE js AS """                                                                                                                                                                                     
      const RANDOMIZATION_SEEDS_TO_TRY = 30;                                                                                                                                                             
      function generateRandomizationSeed() {{
          return Math.floor(Math.random() * 1e10)                                                                                                                                                        
              .toString(16)
              .padStart(9, '0');                                                                                                                                                                         
      }}                                                                                                                                                                                                  
   
      return Array.from({{ length: RANDOMIZATION_SEEDS_TO_TRY }}, () => generateRandomizationSeed());                                                                                                      
  """;             
                                                                                                                                                                                                         
  SET seeds = generateSeeds();

  WITH user_variants AS (
      SELECT
          dt,                                                                                                                                                                                             
          user_id, 
          bucket(
              '{seed}',
              user_id,                                                                                                                                                                                    
              SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(abTestWeightedBucketsBase64))
          ) AS variant                                                                                                                                                                                   
      FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
      join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device using (user_id) 
      WHERE 1=1
          AND dt between daterange_start and daterange_end                                                                                                                                                                       
          AND platform IS NOT NULL
          AND active=1                                                                                                                                                                       
  )

  -- Aggregates daily IAP and ad revenue per user, joined with device/install dimensions                                                                                                                 
 , user_daily_metrics as (
    select                                                                                                                                                                                               
      dt,
      variant,
      count(distinct user_id) as users,
      sum(usd_net_iap_revenue) as iapnetrevenue,
      sum(usd_ad_revenue_est) as adrevenue                                                                                                                                                               
    from user_variants
    join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device using (user_id)                                                                                                         
    left join trailmixgames-game-1.merger_prod_fact.fact_dt_user_iap_revenue using (dt, user_id)                                                                                                         
    left join trailmixgames-game-1.merger_prod_fact.fact_dt_user_ad_revenue using (dt, user_id)                                                                                                          
    where 1=1                                                                                                                                                                                                                                                                                                                                                       
    group by all
    order by dt,variant                                                                                                                                                                                        
  )                                                                                                                                                                                                                                                                                                                                                                                                                                                
  
  select 
  dt,
  variant,
  users,
  iapnetrevenue,
    (iapnetrevenue-iapnetrevenue_trmnt)/iapnetrevenue as iapnetrevenue_diff,
  adrevenue,
    (adrevenue-adrevenue_trmnt)/adrevenue as adrevenue_diff,
  arpdau,
  (arpdau-arpdau_trmnt)/arpdau as arpdau_diff,
    adrpdau,
  (adrpdau-adrpdau_trmnt)/adrpdau as adarpdau_diff
from (

    select
        dt,
        variant,
        users,
        iapnetrevenue,
        lead(iapnetrevenue) over (partition by dt order by dt, variant) as iapnetrevenue_trmnt,
        adrevenue,
        lead(adrevenue) over (partition by dt order by dt, variant) as adrevenue_trmnt,
        iapnetrevenue/users as arpdau,
        lead(iapnetrevenue) over (partition by dt order by dt, variant)/users as arpdau_trmnt,
        adrevenue/users as adrpdau,
        lead(adrevenue) over (partition by dt order by dt, variant)/users as adrpdau_trmnt,
    from user_daily_metrics
)
order by 1,2