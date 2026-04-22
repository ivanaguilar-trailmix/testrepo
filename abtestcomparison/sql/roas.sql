DECLARE lookback INT64 DEFAULT {lookback};


SELECT 
dt
, SUM(usd_cost) AS cost
, SUM(n_playfab_installs) AS installs
, round(SUM(n_playfab_installs_superposition),0) AS installs_super
#IAP
, SUM(usd_net_iap_revenue_total_predicted) AS iaprevenue
, SUM(usd_net_iap_revenue_d1_predicted) AS iaprevenue_d1
, SUM(usd_net_iap_revenue_superposition_total_predicted) AS iaprevenue_super
, SUM(usd_net_iap_revenue_superposition_d1_predicted) AS iaprevenue_super_d1
#IAA
, SUM(usd_iaa_revenue_total_predicted) AS iaarevenue
, SUM(usd_iaa_revenue_d1_predicted) AS iaarevenue_d1
, SUM(usd_iaa_revenue_superposition_total_predicted) AS iaarevenue_super
, SUM(usd_iaa_revenue_superposition_d1_predicted) AS iaarevenue_super_d1
# IAP + IAA
, SUM(usd_net_iap_revenue_total_predicted) + SUM(usd_iaa_revenue_total_predicted) as revenue
, SUM(usd_net_iap_revenue_d1_predicted) + SUM(usd_iaa_revenue_d1_predicted) as revenue_d1
, SUM(usd_net_iap_revenue_superposition_total_predicted) + SUM(usd_iaa_revenue_superposition_total_predicted) as revenue_super
, SUM(usd_net_iap_revenue_superposition_d1_predicted) + SUM(usd_iaa_revenue_superposition_d1_predicted) as revenue_super_d1
# ROAS
, (SUM(usd_net_iap_revenue_total_predicted) + SUM(usd_iaa_revenue_total_predicted))/ SUM(usd_cost) AS roas
, (SUM(usd_net_iap_revenue_d1_predicted) + SUM(usd_iaa_revenue_d1_predicted))/ SUM(usd_cost) AS roas_d1
, (SUM(usd_net_iap_revenue_superposition_total_predicted) + SUM(usd_iaa_revenue_superposition_total_predicted))/ SUM(usd_cost) AS roas_super
, (SUM(usd_net_iap_revenue_superposition_d1_predicted) + SUM(usd_iaa_revenue_superposition_d1_predicted))/ SUM(usd_cost) AS roas_super_d1


FROM trailmixgames-game-1.merger_prod_viz.viz_cohort_marketing_metrics
WHERE dt >= current_date-lookback
GROUP BY ALL 
ORDER BY 1 desc

