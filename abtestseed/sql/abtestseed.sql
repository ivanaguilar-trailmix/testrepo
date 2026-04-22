  -- A/B test start date (defaults to yesterday)
  DECLARE abteststartdate DATE DEFAULT current_date-1;
  -- Date range for averaging metrics (defaults to last {lookback_days} days)                                                                                                                                         
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

  -- Aggregates daily IAP and ad revenue per user, joined with device/install dimensions                                                                                                                 
  WITH user_daily_metrics as (
    select                                                                                                                                                                                               
      dt,         
      user_id as userid,
      platform,
      sum(usd_net_iap_revenue) as iapnetrevenue,
      sum(usd_ad_revenue_est) as adrevenue                                                                                                                                                               
    from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
    join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device using (user_id)                                                                                                             
    left join trailmixgames-game-1.merger_prod_fact.fact_dt_user_iap_revenue using (dt, user_id)                                                                                                         
    left join trailmixgames-game-1.merger_prod_fact.fact_dt_user_ad_revenue using (dt, user_id)                                                                                                          
    where 1=1                                                                                                                                                                                            
    and dt between daterange_start and daterange_end
    and platform is not null
    AND active=1                                                                                                                                                                                
    group by all                                                                                                                                                                                         
  )                                                                                                                                                                                                      
                                                                                                                                                                                                         
  -- Computes key metrics (DAU, payers, ARPDAU) per salt and bucket across all generated seeds,                                                                                                          
  -- averaged over the date range to smooth out daily variance
  ,saltedValues as (                                                                                                                                                                                     
  SELECT          
      salt,                                                                                                                                                                                              
      bucket,     
      AVG(DAU) DAU,                                                                                                                                                                                      
      AVG(DPU_0) DPU_0,                                                                                                                                                                                  
      AVG(DPU_10) DPU_10,
      SUM(iapnetrevenue) as iapnetrevenue,
      SUM(adrevenue) as adrevenue,                                                                                                                                                                                
      AVG(ARPDAU) ARPDAU,                                                                                                                                                                                
      AVG(AD_ARPDAU) AD_ARPDAU                                                                                                                                                                           
  FROM (SELECT                                                                                                                                                                                           
      dt,                                                                                                                                                                                                
      salt,                                                                                                                                                                                              
      bucket(salt, userid, safe_convert_bytes_to_string(from_base64(abTestWeightedBucketsBase64))) as bucket,
      COUNT(DISTINCT userid) AS DAU,                                                                                                                                                                     
      COUNT(DISTINCT
          IF(iapnetrevenue > 0,                                                                                                                                                                          
              userid,                                                                                                                                                                                    
              NULL)) AS DPU_0,
      COUNT(DISTINCT                                                                                                                                                                                     
          IF(iapnetrevenue > 10,                                                                                                                                                                         
              userid,                                                                                                                                                                                    
              NULL)) AS DPU_10,  
      SUM(iapnetrevenue) as iapnetrevenue,
      SUM(adrevenue) as adrevenue,                                                                                                                                                                            
      SUM(iapnetrevenue)/COUNT(DISTINCT userid) AS ARPDAU,                                                                                                                                               
      SUM(adrevenue)/COUNT(DISTINCT userid) AS AD_ARPDAU                                                                                                                                                 
  ## Add client parameters as filters and user segments as a join here                                                                                                                                   
  FROM (                                                                                                                                                                                                 
          SELECT                                                                                                                                                                                         
              userid,                                                                                                                                                                                    
              iapnetrevenue,
              adrevenue,
              dt
          FROM user_daily_metrics AS udm                                                                                                                                                               
          WHERE 1=1)                                                                                                                                                                  
      CROSS JOIN (                                                                                                                                                                                       
      SELECT salt
      FROM UNNEST(seeds)                                                                                                                                                                                 
      AS salt     
  )
      GROUP BY 1,2,3)
  GROUP BY 1,2                                                                                                                                                                                           
  ORDER BY 1),
                                                                                                                                                                                                         
  -- Isolates the control group (bucket = 0) metrics per salt, used as the baseline for lift calculations
  controlValues AS (                                                                                                                                                                                     
      SELECT salt, ARPDAU, AD_ARPDAU
      FROM saltedValues WHERE bucket=0                                                                                                                                                                   
  )
                                                                                                                                                                                                         
                                                                                                                                                                                                         
  -- Final output: per-bucket metrics with ARPDAU and AD_ARPDAU lift vs control, plus date range metadata                                                                                                
  select
   *,                                                                                                                                                                                                    
   ABS(ARPDAU_DIFF) ABS_ARPDAU_DIFF,
   ABS(AD_ARPDAU_DIFF) ABS_AD_ARPDAU_DIFF,                                                                                                                                                               
   abTestWeightedBucketsBase64 as abTestWeightedBucketsBase64,
   daterange_start as dt_start,                                                                                                                                                                          
   daterange_end as dt_end,
  from (                                                                                                                                                                                                 
    SELECT s.*,   
        CASE WHEN s.bucket=0 OR c.ARPDAU=0 THEN 0 ELSE (s.ARPDAU - c.ARPDAU) / ABS(c.ARPDAU) END as ARPDAU_DIFF,                                                                                         
        CASE WHEN s.bucket=0 OR c.AD_ARPDAU=0 THEN 0 ELSE (s.AD_ARPDAU - c.AD_ARPDAU) / ABS(c.AD_ARPDAU) END as AD_ARPDAU_DIFF                                                                           
    FROM saltedValues s LEFT JOIN controlValues c ON s.salt = c.salt ORDER BY salt, bucket ASC                                                                                                           
  )  
  order by salt, bucket, abs_arpdau_diff asc